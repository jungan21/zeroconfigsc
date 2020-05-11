package org.apache.servicecomb.zeroconfigsc.server;

import net.posick.mDNS.ServiceInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ZeroConfigRegistryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigRegistryService.class);

    // register service instance
    public void registerMicroserviceInstance(ServiceInstance mdnsService) {
        String instanceId = (String) mdnsService.getTextAttributes().get(INSTANCE_ID);
        String serviceId = (String) mdnsService.getTextAttributes().get(SERVICE_ID);
        String serviceName = (String) mdnsService.getTextAttributes().get(SERVICE_NAME);

        if ( serviceId == null || serviceName == null || instanceId == null ) {
            LOGGER.error("serviceId: {} is null OR  instanceId: {} is null OR serviceName: {} is null", serviceId, instanceId, serviceName);
            return;
        }

        // convert to server side ServerMicroserviceInstance object
        Optional<ServerMicroserviceInstance> newServerMicroserviceInstance = ServerUtil.convertToServerMicroserviceInstance(mdnsService);

        // add/update in-memory map
        Map<String, ServerMicroserviceInstance> innerInstanceMap = ServerUtil.microserviceInstanceMap.
                computeIfAbsent(serviceId, id -> new ConcurrentHashMap<>());

        // for Client to easily discover the instance's endpoints by serviceName
//        List<ServerMicroserviceInstance> innerInstanceByServiceNameList = ServerUtil.serverMicroserviceInstanceMapByServiceName.
//                computeIfAbsent(serviceName, name -> new ArrayList<>());
//        innerInstanceByServiceNameList.add(newServerMicroserviceInstance.get());

        if (innerInstanceMap.containsKey(instanceId)) {
            LOGGER.error("Failed to register service instance. Because serviceId: {}, instanceId: {} already exists", serviceId, instanceId);
        } else {
            // register a new instance for the service
            LOGGER.info("Register a new instance for  serviceId: {}, instanceId: {}", serviceId, instanceId);
            innerInstanceMap.put(instanceId, newServerMicroserviceInstance.get());
        }

    }

    /**
     * unregister microservice
     *
     * @param microserviceId
     * @param microserviceInstanceId
     */
    public void unregisterMicroserviceInstance(String microserviceId, String microserviceInstanceId) {
        Map<String, ServerMicroserviceInstance> innerInstanceMap = ServerUtil.microserviceInstanceMap.get(microserviceId);
        if (innerInstanceMap != null && innerInstanceMap.containsKey(microserviceInstanceId)){

            ServerMicroserviceInstance instanceToBeRemoved = innerInstanceMap.get(microserviceInstanceId);
            innerInstanceMap.remove(microserviceInstanceId);
            LOGGER.info("Removed service instance from <serviceId, Map<instanceId, instance>>  Map with  microserviceInstanceId: {} ",  microserviceInstanceId);

            // Going to unregister a service instance {hostName=DESKTOP-Q2K46AO, instanceId=c19ddbd1, appId=springmvc-sample, serviceId=16e8633d, serviceName=springmvcConsumer, version=0.0.2, status=UP}
//            List<ServerMicroserviceInstance> innerInstanceByServiceNameList = ServerUtil.serverMicroserviceInstanceMapByServiceName.get(instanceToBeRemoved.getServiceName());
//
//            if (innerInstanceByServiceNameList != null ){
//                innerInstanceByServiceNameList.removeIf(instance -> instance.getInstanceId().equals(microserviceInstanceId) && instance.getServiceId().equals(microserviceId));
//                LOGGER.info("Removed service instance from <serviceName, instanceList> map ",  microserviceInstanceId);
//            }
        } else {
            LOGGER.warn("ServiceId: {},  InstanceId: {} doesn't exist in <serviceId, Map<instanceId, instance>> map", microserviceId, microserviceInstanceId);
        }

    }

    /**
     *  find a service instance based on the service id and instance id
     *
     * @param serviceId
     * @param instanceId
     * @return ServerMicroserviceInstance object
     */
    public Optional<ServerMicroserviceInstance> findServiceInstance(String serviceId, String instanceId) {
        Map<String, ServerMicroserviceInstance>  serverMicroserviceInstanceMap = ServerUtil.microserviceInstanceMap.get(serviceId);
        return serverMicroserviceInstanceMap != null ? Optional.ofNullable(serverMicroserviceInstanceMap.get(instanceId)) : Optional.empty();
    }

    /**
     *  find a list of service instance based on the service id
     *
     * @param consumerId
     * @param providerId
     * @return ServerMicroserviceInstance list
     */
    public Optional<List<ServerMicroserviceInstance>> getMicroserviceInstance(String consumerId, String providerId) {
        Map<String, ServerMicroserviceInstance> instanceIdMap = ServerUtil.microserviceInstanceMap.get(providerId);
        if (instanceIdMap == null) {
            throw new IllegalArgumentException("Invalid serviceId, serviceId=" + providerId);
        }
        return Optional.ofNullable(new ArrayList<>(instanceIdMap.values()));
    }

    /**
     *
     * @param microserviceId
     * @param microserviceInstanceId
     * @return boolean true/false
     */
    public boolean heartbeat(String microserviceId, String microserviceInstanceId) {
        Map<String, ServerMicroserviceInstance>  serverMicroserviceInstanceMap = ServerUtil.microserviceInstanceMap.get(microserviceId);
        return serverMicroserviceInstanceMap != null && serverMicroserviceInstanceMap.containsKey(microserviceInstanceId);
    }

}
