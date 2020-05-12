package org.apache.servicecomb.zeroconfigsc.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ZeroConfigRegistryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigRegistryService.class);

    public void registerMicroserviceInstance(Map<String, String> serviceAttributeMap) {
         LOGGER.info("Start register service: {} ", serviceAttributeMap);

        String instanceId = serviceAttributeMap.get(INSTANCE_ID);
        String serviceId = serviceAttributeMap.get(SERVICE_ID);
        String serviceName = serviceAttributeMap.get(SERVICE_NAME);

        if ( serviceId == null || serviceName == null || instanceId == null ) {
            LOGGER.error("serviceId: {} is null OR  instanceId: {} is null OR serviceName: {} is null", serviceId, instanceId, serviceName);
            return;
        }

        // convert to server side ServerMicroserviceInstance object
        Optional<ServerMicroserviceInstance> newServerMicroserviceInstance = ServerUtil.convertToServerMicroserviceInstance(serviceAttributeMap);

        // add/update in-memory map
        Map<String, ServerMicroserviceInstance> innerInstanceMap = ServerUtil.microserviceInstanceMap.
                computeIfAbsent(serviceId, id -> new ConcurrentHashMap<>());

//        // for Client to easily discover the instance's endpoints by serviceName
//        List<ServerMicroserviceInstance> innerInstanceByServiceNameList = ServerUtil.getserverMicroserviceInstanceMapByServiceName().
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

    public void unregisterMicroserviceInstance(Map<String, String> serviceAttributeMap) {
        String microserviceId = serviceAttributeMap.get(SERVICE_ID);
        String microserviceInstanceId = serviceAttributeMap.get(INSTANCE_ID);

        Map<String, ServerMicroserviceInstance> innerInstanceMap = ServerUtil.microserviceInstanceMap.get(microserviceId);

        if (innerInstanceMap != null && innerInstanceMap.containsKey(microserviceInstanceId)){

            //ServerMicroserviceInstance instanceToBeRemoved = innerInstanceMap.get(microserviceInstanceId);
            innerInstanceMap.remove(microserviceInstanceId);
            LOGGER.info("Removed service instance from <serviceId, Map<instanceId, instance>>  Map with  microserviceInstanceId: {} ",  microserviceInstanceId);

//            // Going to unregister a service instance {hostName=DESKTOP-Q2K46AO, instanceId=c19ddbd1, appId=springmvc-sample, serviceId=16e8633d, serviceName=springmvcConsumer, version=0.0.2, status=UP}
//            List<ServerMicroserviceInstance> innerInstanceByServiceNameList = ServerUtil.getserverMicroserviceInstanceMapByServiceName().get(instanceToBeRemoved.getServiceName());
//
//            if (innerInstanceByServiceNameList != null ){
//                innerInstanceByServiceNameList.removeIf(instance -> instance.getInstanceId().equals(microserviceInstanceId) && instance.getServiceId().equals(microserviceId));
//                LOGGER.info("Removed service instance from <serviceName, instanceList> map ",  microserviceInstanceId);
//            }
        } else {
            LOGGER.warn("ServiceId: {},  InstanceId: {} doesn't exist in <serviceId, Map<instanceId, instance>> map", microserviceId, microserviceInstanceId);
        }

    }

    public Optional<ServerMicroserviceInstance> findServiceInstance(String serviceId, String instanceId) {
        Map<String, ServerMicroserviceInstance>  serverMicroserviceInstanceMap = ServerUtil.microserviceInstanceMap.get(serviceId);
        return serverMicroserviceInstanceMap != null ? Optional.ofNullable(serverMicroserviceInstanceMap.get(instanceId)) : Optional.empty();
    }

    public Optional<List<ServerMicroserviceInstance>> getMicroserviceInstance(String consumerId, String providerId) {
        Map<String, ServerMicroserviceInstance> instanceIdMap = ServerUtil.microserviceInstanceMap.get(providerId);
        if (instanceIdMap == null) {
            throw new IllegalArgumentException("Invalid serviceId, serviceId=" + providerId);
        }
        return Optional.ofNullable(new ArrayList<>(instanceIdMap.values()));
    }

    public void heartbeat(Map<String, String> heartbeatEventMap) {
        String serviceId = heartbeatEventMap.get(SERVICE_ID);
        String instanceId = heartbeatEventMap.get(INSTANCE_ID);
        Map<String, ServerMicroserviceInstance>  serverMicroserviceInstanceMap = ServerUtil.microserviceInstanceMap.get(serviceId);
        if (serverMicroserviceInstanceMap != null && serverMicroserviceInstanceMap.containsKey(instanceId)){
            ServerMicroserviceInstance instance = serverMicroserviceInstanceMap.get(instanceId);
            instance.setLastHeartbeatTimeStamp(Instant.now());
        } else {
            heartbeatEventMap.put(EVENT, REGISTER_EVENT);
            this.registerMicroserviceInstance(heartbeatEventMap);
        }
    }

    // for compability with legacy code
    public boolean heartbeat(String microserviceId, String microserviceInstanceId) {
        Map<String, ServerMicroserviceInstance>  serverMicroserviceInstanceMap = ServerUtil.microserviceInstanceMap.get(microserviceId);
        return serverMicroserviceInstanceMap != null && serverMicroserviceInstanceMap.containsKey(microserviceInstanceId);
    }

    // for compability with legacy code.
    public ServerMicroserviceInstance getMicroservice(String microserviceId) {
        Map<String, ServerMicroserviceInstance> instanceIdMap = ServerUtil.microserviceInstanceMap.get(microserviceId);
        List <ServerMicroserviceInstance> serverMicroserviceInstanceList = new  ArrayList<>(instanceIdMap.values());
       return serverMicroserviceInstanceList.get(0);
    }

    public List<ServerMicroserviceInstance> findServiceInstances(String appId, String serviceName, String strVersionRule, String revision) {
        List<ServerMicroserviceInstance> resultInstanceList = new ArrayList<>();

        for (Map.Entry<String, Map<String, ServerMicroserviceInstance>> entry : ServerUtil.microserviceInstanceMap.entrySet()){
            Map<String, ServerMicroserviceInstance>  instanceIdMap = entry.getValue();
            for (Map.Entry<String, ServerMicroserviceInstance> innerEntry : instanceIdMap.entrySet()) {
                ServerMicroserviceInstance instance = innerEntry.getValue();
                if (appId.equals(instance.getAppId()) && serviceName.equals(instance.getServiceName())){
                    resultInstanceList.add(instance);
                }
            }
        }
        return resultInstanceList;
    }
}
