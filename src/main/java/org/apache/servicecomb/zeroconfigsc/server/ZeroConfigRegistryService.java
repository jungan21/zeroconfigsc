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
        String instanceId = serviceAttributeMap.get(INSTANCE_ID);
        String serviceId = serviceAttributeMap.get(SERVICE_ID);
        String serviceName = serviceAttributeMap.get(SERVICE_NAME);

        if ( serviceId == null || serviceName == null || instanceId == null ) {
            LOGGER.error("serviceId: {} is null OR  instanceId: {} is null OR serviceName: {} is null", serviceId, instanceId, serviceName);
            return;
        }

        // convert to server side ServerMicroserviceInstance object
        Optional<ServerMicroserviceInstance> newServerMicroserviceInstance = ServerUtil.convertToServerMicroserviceInstance(serviceAttributeMap);
        Map<String, ServerMicroserviceInstance> innerInstanceMap = ServerUtil.microserviceInstanceMap.
                computeIfAbsent(serviceId, id -> new ConcurrentHashMap<>());

        if (innerInstanceMap.containsKey(instanceId)) {
            LOGGER.info("ServiceId: {}, instanceId: {} already exists", serviceId, instanceId);
        } else {
            // register a new instance for the service
            LOGGER.info("Register a new instance for  serviceId: {}, instanceId: {}", serviceId, instanceId);
            innerInstanceMap.put(instanceId, newServerMicroserviceInstance.get());
        }
    }

    public void unregisterMicroserviceInstance(Map<String, String> serviceAttributeMap) {
        String unregisterServiceId = serviceAttributeMap.get(SERVICE_ID);
        String unregisterInstanceId = serviceAttributeMap.get(INSTANCE_ID);

        ServerUtil.microserviceInstanceMap.computeIfPresent(unregisterServiceId, (serviceId, instanceIdMap) -> {
            instanceIdMap.computeIfPresent(unregisterInstanceId, (instanceId, instance) -> {
                // remove this instance from inner instance map
                LOGGER.info("Successfully unregistered/remove serviceId: {}, instanceId: {} from server side",  unregisterServiceId, unregisterInstanceId);
                return null;
            });
            // if the inner instance map is empty, remove the service itself from the outer map too
            return !instanceIdMap.isEmpty() ? instanceIdMap : null;
        });
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

    // for scenario: when other service started before this one start
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

    // for compatibility with existing registration workflow
    public boolean heartbeat(String microserviceId, String microserviceInstanceId) {
        Map<String, ServerMicroserviceInstance>  serverMicroserviceInstanceMap = ServerUtil.microserviceInstanceMap.get(microserviceId);
        return serverMicroserviceInstanceMap != null && serverMicroserviceInstanceMap.containsKey(microserviceInstanceId);
    }

    // for compatibility with existing registration workflow
    public ServerMicroserviceInstance getMicroservice(String microserviceId) {
        Map<String, ServerMicroserviceInstance> instanceIdMap = ServerUtil.microserviceInstanceMap.get(microserviceId);
        List <ServerMicroserviceInstance> serverMicroserviceInstanceList = new  ArrayList<>(instanceIdMap.values());
       return serverMicroserviceInstanceList.get(0);
    }

    public List<ServerMicroserviceInstance> findServiceInstances(String appId, String serviceName) {
        List<ServerMicroserviceInstance> resultInstanceList = new ArrayList<>();
        ServerUtil.microserviceInstanceMap.forEach((serviceId, instanceIdMap) -> {
            instanceIdMap.forEach((instanceId, instance) -> {
                // match appId and ServiceName
                if (appId.equals(instance.getAppId()) && serviceName.equals(instance.getServiceName())){
                    resultInstanceList.add(instance);
                }
            });
        });
        return resultInstanceList;
    }
}
