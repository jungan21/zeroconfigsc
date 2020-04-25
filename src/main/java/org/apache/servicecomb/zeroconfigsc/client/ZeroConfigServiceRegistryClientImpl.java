package org.apache.servicecomb.zeroconfigsc.client;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import net.posick.mDNS.Lookup;
import net.posick.mDNS.MulticastDNSService;
import net.posick.mDNS.ServiceInstance;
import net.posick.mDNS.ServiceName;
import org.apache.servicecomb.zeroconfigsc.server.ZeroConfigRegistryService;
import org.apache.servicecomb.zeroconfigsc.server.ServerMicroservice;
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
import org.apache.servicecomb.serviceregistry.version.VersionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ZeroConfigServiceRegistryClientImpl implements ServiceRegistryClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigServiceRegistryClientImpl.class);

    private IpPortManager ipPortManager;
    private MulticastDNSService multicastDNSService;
    private ZeroConfigRegistryService zeroConfigRegistryService;
    private Microservice microserviceItSelf;

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
        return this.microserviceItSelf != null ? this.microserviceItSelf.getServiceId() : null;
    }

    @Override
    public String registerMicroservice(Microservice microservice) {
        // refer to the logic in LocalServiceRegistryClientImpl.java
        String serviceId = microservice.getServiceId();
        if (serviceId == null || serviceId.length() == 0){
            serviceId = ClientUtil.generateServiceId(microservice.getAppId(), microservice.getServiceName());
        }
        // set to local variable so that it can be used to retrieve serviceName/appId/version when registering instance
        this.microserviceItSelf = microservice;
        return serviceId;
    }

    // add loop up because this is also called by consumer to discover provider service
    @Override
    public Microservice getMicroservice(String microserviceId) {
        if (this.microserviceItSelf.getServiceId().equals(microserviceId)){
            return this.microserviceItSelf;
        } else {
            LOGGER.info("Start finding service from MDNS with microserviceId: {}", microserviceId);
            Microservice service = new Microservice();
            Lookup lookup = null;
            try {
                // have to do Fuzzy search as our service's key in MDNS is based on service name not the service Id
                ServiceName mdnsServiceName  = new ServiceName(microserviceId + MDNS_SERVICE_NAME_SUFFIX);
                lookup = new Lookup(mdnsServiceName);
                ServiceInstance[] mdnsServices = lookup.lookupServices();
                for (ServiceInstance mdnsService : mdnsServices) {
                    if (mdnsService != null && mdnsService.getTextAttributes()!= null){
                        LOGGER.info("find service from MDNS with attributes: {}", mdnsService.getTextAttributes().toString());
                        service = ClientUtil.convertMDNSServiceToClientMicroservice(mdnsService);
                        return service;
                    }
                }
            } catch(IOException e){
                LOGGER.error("Failed to create lookup object with error: {}", e);
            } finally {
                if (lookup != null) {
                    try {
                        lookup.close();
                    } catch (IOException e1) {
                        LOGGER.error("Failed to close lookup object with error: {}", e1);
                    }
                }
            }
            return service;
        }
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
        LOGGER.info("isSchemaExist: microserviceId: {}, scehmaId: {}", microserviceId, schemaId);
        List<String> schemaList = this.microserviceItSelf.getSchemas();
        return this.microserviceItSelf.getServiceId().equals(microserviceId) && schemaList != null && schemaList.contains(schemaId);
    }

    @Override
    public boolean registerSchema(String microserviceId, String schemaId, String schemaContent) {
        LOGGER.info("registerSchema: serviceId: {}, scehmaId: {}, SchemaContent: {}", microserviceId, schemaId, schemaContent);

        if (this.microserviceItSelf != null && microserviceId.equals(this.microserviceItSelf.getServiceId())) {
            // put to both schemaMap and schemaIdList
            this.microserviceItSelf.addSchema(schemaId, schemaContent);
            return true;
        } else {
            LOGGER.error("Invalid serviceId! Failed to retrieve Microservice for serviceId {}", microserviceId);
            return false;
        }
    }

    // TODO TODO TODO need to call provider to retrieve provider schema content direclty for consumder to be able to discover the porovider
    @Override
    public String getSchema(String microserviceId, String schemaId) {
        LOGGER.info("getSchema: microserviceId: {}, scehmaId: {}", microserviceId, schemaId);
        if (this.microserviceItSelf.getServiceId().equals(microserviceId)) {
            return this.microserviceItSelf.getSchemaMap().get(schemaId);
        } else {
            //called by consumer to load provider's schema content (very first time)
            // have to discover provider's endpoint and make a call to load schema from provider side directly
            // Currently, just return its own schemaContent, have to be able to retrieve provider's schema later
            return microserviceItSelf.getSchemaMap() != null ? microserviceItSelf.getSchemaMap().get(schemaId) : null;
        }

    }

    // called by consumer to discover/load provider's schema content i.e. SwaggerLoad.loadFromRemote(serviceId, schemaId)
    @Override
    public String getAggregatedSchema(String microserviceId, String schemaId) {
        LOGGER.info("getAggregatedSchema: microserviceId: {}, scehmaId: {}", microserviceId, schemaId);
        return this.getSchema(microserviceId, schemaId);
    }

    @Override
    public Holder<List<GetSchemaResponse>> getSchemas(String microserviceId) {
        // this method is called in MicroserviceRegisterTask.java doRegister()
        Holder<List<GetSchemaResponse>> resultHolder = new Holder<>();
        if (microserviceItSelf == null) {
            LOGGER.error("Invalid serviceId! Failed to retrieve microservice for serviceId {}", microserviceId);
            return resultHolder;
        }
        List<GetSchemaResponse> schemas = new ArrayList<>();
        microserviceItSelf.getSchemaMap().forEach((key, val) -> {
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
            instanceId = ClientUtil.generateServiceInstanceId(instance);
        }

        try {
            // need currentMicroservice object to retrieve serviceName/appID/version attributes for instance to be registered
            Optional<ServiceInstance> optionalServiceInstance = ClientUtil.convertToMDNSServiceInstance(serviceId, instanceId, instance, this.ipPortManager, this.microserviceItSelf);

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
        Optional<List<ServerMicroservice>> optionalServerMicroserviceInstanceList = this.zeroConfigRegistryService.getMicroserviceInstance(consumerId, providerId);
        if (optionalServerMicroserviceInstanceList.isPresent()) {
            microserviceInstanceResultList = optionalServerMicroserviceInstanceList.get().stream().map(serverInstance -> {
                return ClientUtil.convertToClientMicroserviceInstance(serverInstance);}).collect(Collectors.toList());
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
        try {
            LOGGER.info("Start unregister microservice instance. The instance with servcieId: {} instanceId:{}", microserviceId, microserviceInstanceId);
            ServiceName mdnsServiceName = new ServiceName(microserviceId + MDNS_SERVICE_NAME_SUFFIX);
            return this.multicastDNSService.unregister(mdnsServiceName); // broadcast to MDNS
        } catch (IOException e) {
            LOGGER.error("Failed to unregister microservice instance from mdns server. servcieId: {} instanceId:{}", microserviceId, microserviceInstanceId,  e);
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
        LOGGER.info("find service instance for consumerId: {}, serviceName: {}, versionRule: {}, revision: {}", consumerId,serviceName, strVersionRule, revision);
        /**
         *  1. consumerId not the provider service Id
         *  2. called by RefreshableMicroserviceCache.pullInstanceFromServiceCenter when consumer make a call to service provider for the first time
         *
         *  strVersionRule: "0.0.0.0+", revision： null
         *
         *  currently, returnall instance
         */

        MicroserviceInstances resultMicroserviceInstances = new MicroserviceInstances();
        FindInstancesResponse response = new FindInstancesResponse();
        List<MicroserviceInstance> instanceList = new ArrayList<>();

        Lookup lookup = null;
        try {
            ServiceName mdnsServiceName  = new ServiceName(ClientUtil.generateServiceId(appId, serviceName) + MDNS_SERVICE_NAME_SUFFIX);
            lookup = new Lookup(mdnsServiceName);
            ServiceInstance[] mdnsServices = lookup.lookupServices();
            for (ServiceInstance mdnsService : mdnsServices) {
                LOGGER.info("find service instance from MDNS with attributes: {}", mdnsService.getTextAttributes().toString());
                instanceList.add(ClientUtil.convertMDNSServiceToClientMicroserviceInstance(mdnsService));
            }
        } catch(IOException e){
            LOGGER.error("Failed to create lookup object with error: {}", e);
        } finally {
            if (lookup != null) {
                try {
                    lookup.close();
                } catch (IOException e1) {
                    LOGGER.error("Failed to close lookup object with error: {}", e1);
                }
            }
        }

        response.setInstances(instanceList);
        resultMicroserviceInstances.setInstancesResponse(response);

        return resultMicroserviceInstances;
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
        Optional<ServerMicroservice>  optionalServerMicroserviceInstance = this.zeroConfigRegistryService.findServiceInstance(serviceId, instanceId);

        if (optionalServerMicroserviceInstance.isPresent()) {
            return ClientUtil.convertToClientMicroserviceInstance(optionalServerMicroserviceInstance.get());
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
