package org.apache.servicecomb.zeroconfigsc.client;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.MDNS_SERVICE_NAME_SUFFIX;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.INSTANCE_HEARTBEAT_RESPONSE_MESSAGE_OK;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import net.posick.mDNS.MulticastDNSService;
import net.posick.mDNS.ServiceInstance;
import net.posick.mDNS.ServiceName;
import org.apache.servicecomb.zeroconfigsc.server.ZeroConfigRegistryService;
import org.apache.servicecomb.zeroconfigsc.server.ServerMicroserviceInstance;
import org.apache.servicecomb.foundation.vertx.AsyncResultCallback;
import org.apache.servicecomb.serviceregistry.api.registry.*;
import org.apache.servicecomb.serviceregistry.api.response.FindInstancesResponse;
import org.apache.servicecomb.serviceregistry.api.response.GetSchemaResponse;
import org.apache.servicecomb.serviceregistry.api.response.HeartbeatResponse;
import org.apache.servicecomb.serviceregistry.api.response.MicroserviceInstanceChangedEvent;
import org.apache.servicecomb.serviceregistry.client.IpPortManager;
import org.apache.servicecomb.serviceregistry.client.ServiceRegistryClient;
import org.apache.servicecomb.serviceregistry.client.http.Holder;
import org.apache.servicecomb.serviceregistry.client.http.MicroserviceInstances;
import org.apache.servicecomb.serviceregistry.config.ServiceRegistryConfig;
import org.apache.servicecomb.serviceregistry.version.Version;
import org.apache.servicecomb.serviceregistry.version.VersionRule;
import org.apache.servicecomb.serviceregistry.version.VersionRuleUtils;
import org.apache.servicecomb.serviceregistry.version.VersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ZeroConfigServiceRegistryClientImpl implements ServiceRegistryClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigServiceRegistryClientImpl.class);

    private IpPortManager ipPortManager;
    private MulticastDNSService multicastDNSService;
    private ZeroConfigRegistryService zeroConfigRegistryService;
    private Microservice currentMicroservice;

    public ZeroConfigServiceRegistryClientImpl(ServiceRegistryConfig serviceRegistryConfig){
        this.ipPortManager = new IpPortManager(serviceRegistryConfig);
        try {
            this.multicastDNSService = new MulticastDNSService();
        } catch (IOException e) {
            LOGGER.error("Failed to create MulticastDNSService object", e);
        }
        // for query ONLY, for update, we have to broadcast
        this.zeroConfigRegistryService = new ZeroConfigRegistryService();
    }

    @Override
    public void init() {}

    @Override
    public List<Microservice> getAllMicroservices() {
        // TODO in our new Zero Configuration registration center, it refers to all service instances
        List<Microservice> serverMicroserviceList =  new ArrayList<>();
        return serverMicroserviceList;
    }

    // this method is called before Microservice registration to check whether service with this ID exists or not
    @Override
    public String getMicroserviceId(String appId, String microserviceName, String versionRule, String environment) {
        return this.currentMicroservice != null ? this.currentMicroservice.getServiceId() : null;
    }

    @Override
    public String registerMicroservice(Microservice microservice) {
        // refer to the logic in LocalServiceRegistryClientImpl.java
        String serviceId = microservice.getServiceId();
        if (serviceId == null || serviceId.length() == 0){
            serviceId = ZeroConfigRegistryClientUtil.generateServiceId(microservice);
        }
        // set to local variable so that it can be used to retrieve serviceName/appId/version when registering instance
        this.currentMicroservice = microservice;
        return serviceId;
    }

    @Override
    public Microservice getMicroservice(String microserviceId) {
        return this.currentMicroservice.getServiceId().equals(microserviceId) ? this.currentMicroservice : null;
    }

    @Override
    public Microservice getAggregatedMicroservice(String microserviceId) {
        return this.getMicroservice(microserviceId);
    }

    @Override
    public boolean updateMicroserviceProperties(String microserviceId, Map<String, String> serviceProperties) {
        Microservice microservice = this.getMicroservice(microserviceId);
        if(microservice == null) {
            return false;
        }
        // putAll will update values for keys exist in the map, also add new <key, value> to the map
        microservice.getProperties().putAll(serviceProperties);
        String serviceId = this.registerMicroservice(microservice);

        return serviceId != null && !serviceId.isEmpty();
    }

    @Override
    public boolean isSchemaExist(String microserviceId, String schemaId) {
        List<String> schemaList = this.currentMicroservice.getSchemas();
        return this.currentMicroservice.getServiceId().equals(microserviceId) && schemaList != null && schemaList.contains(schemaId);
    }

    @Override
    public boolean registerSchema(String microserviceId, String schemaId, String schemaContent) {
        return true;
    }

    @Override
    public String getSchema(String microserviceId, String schemaId) {
        Microservice microservice = this.getMicroservice(microserviceId);
        if (microservice == null) {
            LOGGER.error("Invalid serviceId! Failed to retrieve microservice for serviceId {}", microserviceId);
            return null;
        }
        return microservice.getSchemaMap() != null ? microservice.getSchemaMap().get(schemaId) : null;
    }

    @Override
    public String getAggregatedSchema(String microserviceId, String schemaId) {
        return this.getSchema(microserviceId, schemaId);
    }

    @Override
    public Holder<List<GetSchemaResponse>> getSchemas(String microserviceId) {
        // this method is called in MicroserviceRegisterTask.java doRegister()
        Holder<List<GetSchemaResponse>> resultHolder = new Holder<>();
        if (currentMicroservice == null) {
            LOGGER.error("Invalid serviceId! Failed to retrieve microservice for serviceId {}", microserviceId);
            return resultHolder;
        }
        List<GetSchemaResponse> schemas = new ArrayList<>();
        currentMicroservice.getSchemaMap().forEach((key, val) -> {
            GetSchemaResponse schema = new GetSchemaResponse();
            schema.setSchema(val);
            schema.setSchemaId(key);
            schema.setSummary(Hashing.sha256().newHasher().putString(val, Charsets.UTF_8).hash().toString());
            schemas.add(schema);
        });
        resultHolder.setStatusCode(Response.Status.OK.getStatusCode()).setValue(schemas);
        return resultHolder;
    }

    @Override
    public String registerMicroserviceInstance(MicroserviceInstance instance) {
        String serviceId = instance.getServiceId();
        String instanceId = instance.getInstanceId(); // allow client to set the instanceId
        if (instanceId == null || instanceId.length() == 0){
            instanceId = ZeroConfigRegistryClientUtil.generateServiceInstanceId(instance);
        }

        try {
            // need currentMicroservice object to retrieve serviceName/appID/version attributes for instance to be registered
            Optional<ServiceInstance> optionalServiceInstance = ZeroConfigRegistryClientUtil.convertToMDNSServiceInstance(serviceId, instanceId, instance, this.ipPortManager, this.currentMicroservice);

            if (optionalServiceInstance.isPresent()){
                this.multicastDNSService.register(optionalServiceInstance.get()); // broadcast to MDNS
            } else {
                LOGGER.error("Failed to register microservice instance. Because optionalServiceInstance is null/empty: {}", optionalServiceInstance);
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("Failed to register microservice instance to mdns. servcieId: {} instanceId:{}", serviceId, instanceId,  e);
            return null;
        }
        return instanceId;
    }

    @Override
    public List<MicroserviceInstance> getMicroserviceInstance(String consumerId, String providerId) {
        List<MicroserviceInstance> microserviceInstanceResultList = new ArrayList<>();
        Optional<List<ServerMicroserviceInstance>> optionalServerMicroserviceInstanceList = this.zeroConfigRegistryService.getMicroserviceInstance(consumerId, providerId);
        if (optionalServerMicroserviceInstanceList.isPresent()) {
            microserviceInstanceResultList = optionalServerMicroserviceInstanceList.get().stream().map(serverInstance -> {
                return ZeroConfigRegistryClientUtil.convertToClientMicroserviceInstance(serverInstance);}).collect(Collectors.toList());
        } else {
            LOGGER.error("Invalid serviceId: {}", providerId);
        }
        return microserviceInstanceResultList;
    }

    @Override
    public boolean updateInstanceProperties(String microserviceId, String microserviceInstanceId, Map<String, String> instanceProperties) {
        MicroserviceInstance microserviceInstance = this.findServiceInstance(microserviceId, microserviceInstanceId);
        if(microserviceInstance == null) {
            LOGGER.error("Invalid microserviceId, microserviceId: {} OR microserviceInstanceId, microserviceInstanceId: {}", microserviceId, microserviceInstanceId);
            return false;
        }

        if( microserviceInstance.getProperties().equals(instanceProperties)) {
            throw new IllegalArgumentException("No update to existing instance properties" +  instanceProperties);
        }

        // putAll will update values for keys exist in the map, also add new <key, value> to the map
        microserviceInstance.getProperties().putAll(instanceProperties);

        String serviceInstanceId = this.registerMicroserviceInstance(microserviceInstance);

        return serviceInstanceId != null && !serviceInstanceId.isEmpty();
    }

    @Override
    public boolean unregisterMicroserviceInstance(String microserviceId, String microserviceInstanceId) {
        Optional<ServerMicroserviceInstance> optionalServerMicroserviceInstance =  this.zeroConfigRegistryService.findServiceInstance(microserviceId, microserviceInstanceId);

        if (optionalServerMicroserviceInstance.isPresent()) {
            try {
                // convention to append "._http._tcp.local."
                LOGGER.info("Start unregister microservice instance. The instance with servcieId: {} instanceId:{}", microserviceId, microserviceInstanceId);
                ServiceName mdnsServiceName = new ServiceName(optionalServerMicroserviceInstance.get().getServiceName() + MDNS_SERVICE_NAME_SUFFIX);
                // broadcast to MDNS
                return this.multicastDNSService.unregister(mdnsServiceName);
            } catch (IOException e) {
                LOGGER.error("Failed to unregister microservice instance from mdns server. servcieId: {} instanceId:{}", microserviceId, microserviceInstanceId,  e);
                return false;
            }

        } else {
            LOGGER.error("Failed to unregister microservice instance from mdns server. The instance with servcieId: {} instanceId:{} doesn't exist in MDNS server", microserviceId, microserviceInstanceId);
            return false;
        }

    }

    @Override
    public HeartbeatResponse heartbeat(String microserviceId, String microserviceInstanceId) {
        HeartbeatResponse response = new HeartbeatResponse();
        if (this.zeroConfigRegistryService.heartbeat(microserviceId, microserviceInstanceId)) {
            response.setMessage(INSTANCE_HEARTBEAT_RESPONSE_MESSAGE_OK);
            response.setOk(true);
        }
        return response;
    }

    @Override
    public void watch(String selfMicroserviceId, AsyncResultCallback<MicroserviceInstanceChangedEvent> callback) {}

    @Override
    public void watch(String selfMicroserviceId, AsyncResultCallback<MicroserviceInstanceChangedEvent> callback, AsyncResultCallback<Void> onOpen, AsyncResultCallback<Void> onClose) {}

    @Override
    public List<MicroserviceInstance> findServiceInstance(String consumerId, String appId, String serviceName, String versionRule) {
        MicroserviceInstances instances = findServiceInstances(consumerId, appId, serviceName, versionRule, null);
        if (instances.isMicroserviceNotExist()) {
            return null;
        }
        return instances.getInstancesResponse().getInstances();
    }

    @Override
    public MicroserviceInstances findServiceInstances(String consumerId, String appId, String serviceName, String strVersionRule, String revision) {
        int currentRevision = 1;

        List<MicroserviceInstance> allInstances = new ArrayList<>();
        MicroserviceInstances microserviceInstances = new MicroserviceInstances();
        FindInstancesResponse response = new FindInstancesResponse();
        if (revision != null && currentRevision == Integer.parseInt(revision)) {
            microserviceInstances.setNeedRefresh(false);
            return microserviceInstances;
        }

        microserviceInstances.setRevision(String.valueOf(currentRevision));
        VersionRule versionRule = VersionRuleUtils.getOrCreate(strVersionRule);
        Microservice latestMicroservice = findLatest(appId, serviceName, versionRule);
        if (latestMicroservice == null) {
            microserviceInstances.setMicroserviceNotExist(true);
            return microserviceInstances;
        }

        Version latestVersion = VersionUtils.getOrCreate(latestMicroservice.getVersion());
        for (Microservice microservice : this.getAllMicroservices()) {
            if (!isSameMicroservice(microservice, appId, serviceName)) {
                continue;
            }

            Version version = VersionUtils.getOrCreate(microservice.getVersion());
            if (!versionRule.isMatch(version, latestVersion)) {
                continue;
            }

            List<MicroserviceInstance> microserviceInstanceList = this.getMicroserviceInstance(null, microservice.getServiceId());
            allInstances.addAll(microserviceInstanceList);
        }
        response.setInstances(allInstances);
        microserviceInstances.setInstancesResponse(response);

        return microserviceInstances;
    }



    private Microservice findLatest(String appId, String serviceName, VersionRule versionRule) {
        Version latestVersion = null;
        Microservice latest = null;
        List<Microservice> microserviceList = this.getAllMicroservices();
        for (Microservice microservice : microserviceList) {
            if (!isSameMicroservice(microservice, appId, serviceName)) {
                continue;
            }
            Version version = VersionUtils.getOrCreate(microservice.getVersion());
            if (!versionRule.isAccept(version)) {
                continue;
            }
            if (latestVersion == null || version.compareTo(latestVersion) > 0) {
                latestVersion = version;
                latest = microservice;
            }
        }

        return latest;
    }

    private boolean isSameMicroservice(Microservice microservice, String appId, String serviceName) {
        return microservice.getAppId().equals(appId) && microservice.getServiceName().equals(serviceName);
    }

    @Override
    public MicroserviceInstance findServiceInstance(String serviceId, String instanceId) {
        Optional<ServerMicroserviceInstance>  optionalServerMicroserviceInstance = this.zeroConfigRegistryService.findServiceInstance(serviceId, instanceId);

        if (optionalServerMicroserviceInstance.isPresent()) {
            return ZeroConfigRegistryClientUtil.convertToClientMicroserviceInstance(optionalServerMicroserviceInstance.get());
        } else {
            LOGGER.error("Invalid serviceId OR instanceId! Failed to retrieve Microservice Instance for serviceId {} and instanceId {}", serviceId, instanceId);
            return null;
        }
    }

    @Override
    public ServiceCenterInfo getServiceCenterInfo() {
        ServiceCenterInfo info = new ServiceCenterInfo();
        info.setVersion("1.0.0");
        info.setBuildTag("20200501");
        info.setRunMode("dev");
        info.setApiVersion("1.0.0");
        info.setConfig(new ServiceCenterConfig());
        return info;
    }

    @Override
    public boolean updateMicroserviceInstanceStatus(String microserviceId, String instanceId, MicroserviceInstanceStatus status) {
        if (null == status) {
            throw new IllegalArgumentException("null status is now allowed");
        }

        MicroserviceInstance microserviceInstance = this.findServiceInstance(microserviceId, instanceId);

        if(microserviceInstance == null) {
            throw new IllegalArgumentException("Invalid microserviceId=" +  microserviceId + "OR instanceId=" + instanceId);
        }

        if (status.equals(microserviceInstance.getStatus())){
            throw new IllegalArgumentException("service instance status is same as server side existing status: " +  microserviceInstance.getStatus().toString());
        }

        LOGGER.debug("update status of microservice instance: {}", status);
        microserviceInstance.setStatus(status);
        String serviceInstanceId = this.registerMicroserviceInstance(microserviceInstance);
        return serviceInstanceId != null && !serviceInstanceId.isEmpty();
    }

}
