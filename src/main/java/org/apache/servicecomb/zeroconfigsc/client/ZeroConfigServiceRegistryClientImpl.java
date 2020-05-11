package org.apache.servicecomb.zeroconfigsc.client;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import net.posick.mDNS.Lookup;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.web.client.RestTemplate;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ZeroConfigServiceRegistryClientImpl implements ServiceRegistryClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigServiceRegistryClientImpl.class);

    private IpPortManager ipPortManager;
    private MulticastDNSService multicastDNSService;
    private ZeroConfigRegistryService zeroConfigRegistryService;

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

    // TODO in our new Zero Configuration registration center, it refers to all service instances
    @Override
    public List<Microservice> getAllMicroservices() {
        List<Microservice> serverMicroserviceList =  new ArrayList<>();
        return serverMicroserviceList;
    }

    @Override
    public String getMicroserviceId(String appId, String microserviceName, String strVersionRule, String environment) {
        return ClientUtil.microserviceItSelf.getServiceId();
    }

    @Override
    public String registerMicroservice(Microservice microservice) {
        // refer to the logic in LocalServiceRegistryClientImpl.java
        String serviceId = microservice.getServiceId();
        if (serviceId == null || serviceId.length() == 0){
            // TODO version?
            serviceId = ClientUtil.generateServiceId(microservice.getAppId(), microservice.getServiceName());
            microservice.setServiceId(serviceId);
        }
        // set to local variable so that it can be used to retrieve serviceName/appId/version when registering instance
        ClientUtil.microserviceItSelf = microservice;
        return serviceId;
    }

    /**
     * loop up because this is not only called by service registration task, but also called by consumer
     * to discover provider service for the very first time and populate MicroserviceCache.getService(serviceId)
     *
     * @param microserviceId
     * @return Microservice microservice
     */
    @Override
    public Microservice getMicroservice(String microserviceId) {
        // for registration
        if (ClientUtil.microserviceItSelf.getServiceId().equals(microserviceId)){
            return ClientUtil.microserviceItSelf;
        } else {
            // for consumer to find the provider service for the very first time
            LOGGER.info("Start looking up service from MDNS with microserviceId: {}", microserviceId);
            Microservice service = new Microservice();
            Lookup lookup = null;
            try {
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

    // only used in legacy UT code. Only support updating microserviceitself properties.
    @Override
    public boolean updateMicroserviceProperties(String microserviceId, Map<String, String> serviceProperties) {
        if(microserviceId != ClientUtil.microserviceItSelf.getServiceId()) {
            return false;
        }
        // putAll will update values for keys exist in the map, also add new <key, value> to the map
        ClientUtil.microserviceItSelf.getProperties().putAll(serviceProperties);
        return true;
    }

    @Override
    public boolean isSchemaExist(String microserviceId, String schemaId) {
        LOGGER.info("isSchemaExist: microserviceId: {}, scehmaId: {}", microserviceId, schemaId);
        String serviceId = ClientUtil.microserviceItSelf.getServiceId();
        if (serviceId == null || (serviceId != null && !serviceId.equals(microserviceId))) {
            throw new IllegalArgumentException("Invalid serviceId, serviceId=" + microserviceId);
        }

        List<String> schemaList = ClientUtil.microserviceItSelf.getSchemas();
        return schemaList != null && schemaList.contains(schemaId);
    }

    @Override
    public boolean registerSchema(String microserviceId, String schemaId, String schemaContent) {
        LOGGER.info("registerSchema: serviceId: {}, scehmaId: {}, SchemaContent: {}", microserviceId, schemaId, schemaContent);

        String serviceId = ClientUtil.microserviceItSelf.getServiceId();
        if (serviceId == null || (serviceId != null && !serviceId.equals(microserviceId))) {
            throw new IllegalArgumentException("Invalid serviceId, serviceId=" + microserviceId);
        }

        ClientUtil.microserviceItSelf.addSchema(schemaId, schemaContent);
        return true;
    }

    @Override
    public String getSchema(String microserviceId, String schemaId) {
        LOGGER.info("getSchema: microserviceId: {}, scehmaId: {}", microserviceId, schemaId);
        // called by service registration task when registering itself
        if (ClientUtil.microserviceItSelf.getServiceId().equals(microserviceId)) {
            return  ClientUtil.microserviceItSelf.getSchemaMap().computeIfPresent(schemaId,  (k, v) -> { return v; });
        } else {
           // called by consumer to load provider's schema content for the very first time
            String endpoint = this.getEndpointForMicroservice(microserviceId);
            LOGGER.info("Found the endpoint: {} for the microserviceId: {}", endpoint, microserviceId);
            String schemaContentEndpoint = endpoint + SCHEMA_CONTENT_ENDPOINT_BASE_PATH + SCHEMA_CONTENT_ENDPOINT_SUBPATH + "?" + SCHEMA_CONTENT_ENDPOINT_QUERY_KEYWORD + "=" + schemaId;
            LOGGER.info("Going to retrieve schema content from enpoint:{}", schemaContentEndpoint);
            // Make a rest call to provider's endpoint directly to retrieve the schema content
            String schemaContent = new RestTemplate().postForObject(schemaContentEndpoint, null, String.class);
            LOGGER.info("Retrieved the schema content for microserviceId: {}, schemaId: {}, schemaContent: {}", microserviceId, schemaId, schemaContent);
            return schemaContent;
        }
    }

    private String getEndpointForMicroservice(String microserviceId){
        String endpoint = null;
        Lookup lookup = null;
        try {
            ServiceName mdnsServiceName  = new ServiceName(microserviceId + MDNS_SERVICE_NAME_SUFFIX);
            lookup = new Lookup(mdnsServiceName);
            ServiceInstance[] services = lookup.lookupServices();
            for (ServiceInstance service : services) {
                Map<String, String> attributesMap = service.getTextAttributes();
                if (attributesMap != null && attributesMap.containsKey(ENDPOINTS)) {
                    String tempEndpoint = attributesMap.get(ENDPOINTS);
                    if (!tempEndpoint.contains(SCHEMA_ENDPOINT_LIST_SPLITER)){
                        endpoint = tempEndpoint.replace(ENDPOINT_PREFIX_REST, ENDPOINT_PREFIX_HTTP);
                    } else {
                        endpoint = tempEndpoint.split("\\$")[0].replace(ENDPOINT_PREFIX_REST, ENDPOINT_PREFIX_HTTP);
                    }
                }
                break;
            }
        } catch (IOException e) {
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
        return endpoint;
    }

    // called by consumer to discover/load provider's schema content i.e. SwaggerLoad.loadFromRemote(serviceId, schemaId)
    @Override
    public String getAggregatedSchema(String microserviceId, String schemaId) {
        LOGGER.info("getAggregatedSchema: microserviceId: {}, scehmaId: {}", microserviceId, schemaId);
        return this.getSchema(microserviceId, schemaId);
    }

    @Override
    public Holder<List<GetSchemaResponse>> getSchemas(String microserviceId) {
        // this method is called in MicroserviceRegisterTask.java doRegister(), so it's safe for retrieve it from microserviceItSelf object
        Holder<List<GetSchemaResponse>> resultHolder = new Holder<>();
        if (ClientUtil.microserviceItSelf.getServiceId() != microserviceId) {
            LOGGER.error("Invalid serviceId! Failed to retrieve microservice for serviceId {}", microserviceId);
            return resultHolder;
        }
        List<GetSchemaResponse> schemas = new ArrayList<>();
        ClientUtil.microserviceItSelf.getSchemaMap().forEach((key, val) -> {
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
            instance.setInstanceId(instanceId);
        }

        try {
            // need currentMicroservice object to retrieve serviceName/appID/version attributes for instance to be registered
            Optional<ServiceInstance> optionalServiceInstance = ClientUtil.convertToMDNSServiceInstance(serviceId, instance,
                    this.ipPortManager,  ClientUtil.microserviceItSelf);

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

    // only used in legacy UT code. get microservice instances for providerId,
    @Override
    public List<MicroserviceInstance> getMicroserviceInstance(String consumerId, String providerId) {
        List<MicroserviceInstance> microserviceInstanceResultList = new ArrayList<>();
        Optional<List<ServerMicroserviceInstance>> optionalServerMicroserviceInstanceList = this.zeroConfigRegistryService.getMicroserviceInstance(consumerId, providerId);
        if (optionalServerMicroserviceInstanceList.isPresent()) {
            microserviceInstanceResultList = optionalServerMicroserviceInstanceList.get().stream().map(serverInstance -> {
                return ClientUtil.convertToClientMicroserviceInstance(serverInstance);}).collect(Collectors.toList());
        } else {
            LOGGER.error("Invalid serviceId: {}", providerId);
        }
        return microserviceInstanceResultList;
    }

    // only used in legacy UT code. Only support updating microserviceitself instance properties.
    @Override
    public boolean updateInstanceProperties(String microserviceId, String instanceId, Map<String, String> instanceProperties) {
        Microservice selfMicroservice = ClientUtil.microserviceItSelf;
        MicroserviceInstance selfInstance = selfMicroservice.getInstance();
        if (selfInstance == null || !(selfInstance.getInstanceId().equals(instanceId)) || !(selfMicroservice.getServiceId().equals(microserviceId))) {
            LOGGER.error("Invalid microserviceId, microserviceId: {} OR microserviceInstanceId, microserviceInstanceId: {}", microserviceId, instanceId);
            return false;
        }

        // putAll will update values for keys exist in the map, also add new <key, value> to the map
        selfInstance.getProperties().putAll(instanceProperties);
        ClientUtil.microserviceItSelf.setInstance(selfInstance);
        return true;
    }

    // TODO TODO TODO  unregister a specific instance of a service instead of all instance of a service
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
    public List<MicroserviceInstance> findServiceInstance(String selfMicroserviceId, String appId, String serviceName, String versionRule) {
        MicroserviceInstances instances = findServiceInstances(selfMicroserviceId, appId, serviceName, versionRule, null);
        if (instances.isMicroserviceNotExist()) {
            return null;
        }
        return instances.getInstancesResponse().getInstances();
    }

    // TODO TODO TOTO
    @Override
    public MicroserviceInstances findServiceInstances(String consumerId, String appId, String providerServiceName, String strVersionRule, String revision) {
        LOGGER.info("find service instance for consumerId: {}, providerServiceName: {}, versionRule: {}, revision: {}", consumerId,providerServiceName, strVersionRule, revision);
        /**
         *  1. consumerId not the provider service Id
         *  2. called by RefreshableMicroserviceCache.pullInstanceFromServiceCenter when consumer make a call to service provider for the first time
         *
         *  strVersionRule: "0.0.0.0+", revision： null
         *
         *  currently, return all instance
         */

        MicroserviceInstances resultMicroserviceInstances = new MicroserviceInstances();
        FindInstancesResponse response = new FindInstancesResponse();
        List<MicroserviceInstance> instanceList = new ArrayList<>();

        Lookup lookup = null;
        try {
            ServiceName mdnsServiceName  = new ServiceName(ClientUtil.generateServiceId(appId, providerServiceName) + MDNS_SERVICE_NAME_SUFFIX);
            //ServiceName mdnsServiceName  = new ServiceName(MDNS_SERVICE_QUERY_KEYWORD);
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

    private boolean isSameMicroservice(Microservice microservice, String appId, String serviceName) {
        return microservice.getAppId().equals(appId) && microservice.getServiceName().equals(serviceName);
    }

    // called only by MicroserviceInstanceRegisterTask.java. (right after service instance registration task, it use returned instanceID and serviceId to query)
    @Override
    public MicroserviceInstance findServiceInstance(String serviceId, String instanceId) {
        Optional<ServerMicroserviceInstance>  optionalServerMicroserviceInstance = this.zeroConfigRegistryService.findServiceInstance(serviceId, instanceId);

        if (optionalServerMicroserviceInstance.isPresent()) {
            return ClientUtil.convertToClientMicroserviceInstance(optionalServerMicroserviceInstance.get());
        } else {
            LOGGER.error("Invalid serviceId OR instanceId! Failed to retrieve Microservice Instance for serviceId {} and instanceId {}", serviceId, instanceId);
            return null;
        }
    }

    // for compatibility. only used in the legacy UT code.
    @Override
    public ServiceCenterInfo getServiceCenterInfo() {
        ServiceCenterInfo info = new ServiceCenterInfo();
        info.setVersion("");
        info.setBuildTag("");
        info.setRunMode("");
        info.setApiVersion("");
        info.setConfig(new ServiceCenterConfig());
        return info;
    }

    /**
     *  for compatibility.
     * 1. Only called by SCBEngine.turnDownInstanceStatus to set instance status to Down
     * 2. In zero-config context, there is no need to update status (from UP TO DOWN)as there is no physical registry center to show the status
     *
      */

    @Override
    public boolean updateMicroserviceInstanceStatus(String microserviceId, String instanceId, MicroserviceInstanceStatus status) {
        if (null == status) {
            throw new IllegalArgumentException("null status is now allowed");
        }
        String selfServiceId = ClientUtil.microserviceItSelf.getServiceId();
        MicroserviceInstance selfInstance = ClientUtil.microserviceItSelf.getInstance();

        if (!microserviceId.equals(selfServiceId) || selfInstance == null || !selfInstance.getInstanceId().equals(instanceId)){
            throw new IllegalArgumentException(
                    String.format("Invalid argument. microserviceId=%s, instanceId=%s.",
                            microserviceId,
                            instanceId));
        }
        selfInstance.setStatus(status);
        this.unregisterMicroserviceInstance(microserviceId, instanceId);
        return true;
    }

}
