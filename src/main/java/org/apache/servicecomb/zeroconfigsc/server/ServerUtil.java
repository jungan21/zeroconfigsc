package org.apache.servicecomb.zeroconfigsc.server;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SCHEMA_ENDPOINT_LIST_SPLITER;

import net.posick.mDNS.ServiceInstance;
import net.posick.mDNS.MulticastDNSService;
import net.posick.mDNS.Browse;
import net.posick.mDNS.DNSSDListener;
import org.apache.servicecomb.serviceregistry.api.registry.Microservice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Message;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ServerUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerUtil.class);

    private static ZeroConfigRegistryService zeroConfigRegistryService;

    // 1st key: serviceId, 2nd key: instanceId
    public static Map<String, Map<String, ServerMicroserviceInstance>>  microserviceInstanceMap = new ConcurrentHashMap<>();

    // Key: serviceId
    private Map<String, Microservice> microserviceIdMap = new ConcurrentHashMap<>();

    // key: serviceName, 2nd key: Version
    //public static Map<String, List<ServerMicroserviceInstance>>  serverMicroserviceInstanceMapByServiceName = new ConcurrentHashMap<>();

    public static synchronized void init() {
        zeroConfigRegistryService = new ZeroConfigRegistryService();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            startAsyncListenerForRegisteredServices();
        });

    }

    public static Optional<ServerMicroserviceInstance> convertToServerMicroserviceInstance(ServiceInstance serviceInstance){
        Map<String, String> serviceInstanceTextAttributesMap = serviceInstance.getTextAttributes();
        return  Optional.of(buildServerMicroserviceInstanceFromMap(serviceInstanceTextAttributesMap));
    }

    private static ServerMicroserviceInstance buildServerMicroserviceInstanceFromMap (Map<String, String> map) {
        ServerMicroserviceInstance serverMicroserviceInstance = new ServerMicroserviceInstance();
        serverMicroserviceInstance.setInstanceId(map.get(INSTANCE_ID));
        serverMicroserviceInstance.setServiceId(map.get(SERVICE_ID));
        serverMicroserviceInstance.setStatus(map.get(STATUS));
        serverMicroserviceInstance.setHostName(map.get(HOST_NAME));
        serverMicroserviceInstance.setAppId(map.get(APP_ID));
        serverMicroserviceInstance.setServiceName(map.get(SERVICE_NAME));
        serverMicroserviceInstance.setVersion(map.get(VERSION));

        // rest://127.0.0.1:8080$rest://127.0.0.1:8081
        String endPointsString = map.get(ENDPOINTS);
        if ( endPointsString != null && !endPointsString.isEmpty()){
            if (endPointsString.contains(SCHEMA_ENDPOINT_LIST_SPLITER)){
                serverMicroserviceInstance.setEndpoints(Arrays.asList(endPointsString.split("\\$")));
            } else {
                List<String> list  = new ArrayList<>();
                list.add(endPointsString);
                serverMicroserviceInstance.setEndpoints(list);
            }
        }

        // schemaId1$schemaId2
        String schemaIdsString = map.get(SCHEMA_IDS);
        if ( schemaIdsString != null && !schemaIdsString.isEmpty()){
            if (schemaIdsString.contains(SCHEMA_ENDPOINT_LIST_SPLITER)){
                serverMicroserviceInstance.setEndpoints(Arrays.asList(endPointsString.split("\\$")));
            } else {
                List<String> list  = new ArrayList<>();
                list.add(schemaIdsString);
                serverMicroserviceInstance.setSchemas(list);
            }
        }


        return serverMicroserviceInstance;
    }

    private static void startAsyncListenerForRegisteredServices () {

        try {

            MulticastDNSService service = new MulticastDNSService();
            service.startServiceDiscovery(new Browse(DISCOVER_SERVICE_TYPES), new DNSSDListener() {

                // called when a service is registered to MDNS
                public void serviceDiscovered(Object id, ServiceInstance service) {
                    LOGGER.info("Going to register a service instance {}", service.getName().toString());
                    if (service != null && service.getTextAttributes() != null && !service.getTextAttributes().isEmpty()) {
                        Map<String, String> serviceTextAttributesMap = service.getTextAttributes();
                        zeroConfigRegistryService.registerMicroserviceInstance(service);
                        LOGGER.info("Microservice Instance is registered to MDNS server {}", serviceTextAttributesMap);

                        // for debug start register
                        System.out.println("Jun Debug  microserviceInstanceMap:" + ServerUtil.microserviceInstanceMap);
                       // System.out.println("Jun Debug instanceByNameMap register: " + ServerUtil.serverMicroserviceInstanceMapByServiceName);
                        // for debug start register

                    } else {
                        LOGGER.error("Failed to register service instance. Because service's text attributes: {} is null", service.getTextAttributes());
                    }
                }

                // called when a service is unregistered from MDNS OR service process is killed
                public void serviceRemoved(Object id, ServiceInstance service) {
                    LOGGER.info("Going to unregister a service instance {}", service.getTextAttributes());
                    if (service != null && service.getTextAttributes() != null && !service.getTextAttributes().isEmpty()) {
                        Map<String, String> serviceTextAttributesMap = service.getTextAttributes();
                        zeroConfigRegistryService.unregisterMicroserviceInstance(serviceTextAttributesMap.get(SERVICE_ID), serviceTextAttributesMap.get(INSTANCE_ID));
                        LOGGER.info("Microservice Instance is unregistered from MDNS server {}", service.getTextAttributes());

                        // for debug start unregister
                        System.out.println("Jun Debug instanceMap unregister: " + ServerUtil.microserviceInstanceMap);
                       // Map<String, List<ServerMicroserviceInstance>> instanceByNameMap = ServerUtil.serverMicroserviceInstanceMapByServiceName;
                       // System.out.println("Jun Debug instanceByNameMap unregister: " + instanceByNameMap);
                        // for debug start unregister
                    } else {
                        LOGGER.error("Failed to unregister service as service: {} is null OR service's text attributes is null", service.getTextAttributes());
                    }
                }

                public void handleException(Object id, Exception e) {
                    LOGGER.error("Running into errors when registering/unregistering to/from MDNS service registry center", e);
                }

                public void receiveMessage(Object id, Message message) {
                    //LOGGER.warn("Ignore receivedMessage from MDNS");
                }
            });

        } catch (IOException ioException) {
            LOGGER.error("Failed to start Async Service Register/Unregister Listener for MDNS service registry center", ioException);
        }

    }
}
