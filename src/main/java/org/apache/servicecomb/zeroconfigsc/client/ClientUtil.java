package org.apache.servicecomb.zeroconfigsc.client;

import org.apache.servicecomb.zeroconfigsc.server.ServerMicroserviceInstance;
import org.apache.servicecomb.serviceregistry.api.registry.Microservice;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstance;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstanceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ClientUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtil.class);

    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public static Microservice microserviceSelf = new Microservice();

    public static Map<String, String> serviceInstanceMapForHeartbeat = null;

    public static synchronized void init(){
        Runnable heartbeatRunnable = new Runnable() {
            @Override
            public void run() {
                LOGGER.info("Client side runs scheduled heartbeat event for serviceInstanceMapForHeartbeat: {}", serviceInstanceMapForHeartbeat);
                // after first registration succeeds
                if (serviceInstanceMapForHeartbeat != null && !serviceInstanceMapForHeartbeat.isEmpty()){
                    try {
                        byte[] heartbeatEventDataBytes = serviceInstanceMapForHeartbeat.toString().getBytes();
                        MulticastSocket multicastSocket = new MulticastSocket();
                        multicastSocket.setLoopbackMode(false);
                        multicastSocket.setTimeToLive(255);

                        DatagramPacket instanceDataPacket = new DatagramPacket(heartbeatEventDataBytes, heartbeatEventDataBytes.length,
                                InetAddress.getByName(GROUP), PORT);

                        multicastSocket.send(instanceDataPacket);
                    } catch (Exception e) {
                        LOGGER.error("Failed to send heartbeat event", e);
                    }
                }
            }
        };
        executor.scheduleAtFixedRate(heartbeatRunnable, 2, 3, TimeUnit.SECONDS);
    }


    public static Optional<Map<String, String>> convertToRegisterDataModel(String serviceId, String microserviceInstanceId, MicroserviceInstance microserviceInstance, Microservice microservice) {
        Map<String, String> serviceInstanceTextAttributesMap = new HashMap<>();

        serviceInstanceTextAttributesMap.put(EVENT, REGISTER_EVENT);
        serviceInstanceTextAttributesMap.put(VERSION, microservice.getVersion());
        serviceInstanceTextAttributesMap.put(SERVICE_ID, serviceId);
        serviceInstanceTextAttributesMap.put(INSTANCE_ID, microserviceInstanceId);
        serviceInstanceTextAttributesMap.put(STATUS, microserviceInstance.getStatus().toString());
        serviceInstanceTextAttributesMap.put(APP_ID, microservice.getAppId());
        serviceInstanceTextAttributesMap.put(SERVICE_NAME, microservice.getServiceName());
        serviceInstanceTextAttributesMap.put(VERSION, microservice.getVersion());

        String hostName = microserviceInstance.getHostName();
        serviceInstanceTextAttributesMap.put(HOST_NAME, hostName);

        // schema1$schema2
        serviceInstanceTextAttributesMap.put(ENDPOINTS, convertListToString(microserviceInstance.getEndpoints()));
        serviceInstanceTextAttributesMap.put(SCHEMA_IDS, convertListToString(microservice.getSchemas()));

        return Optional.of(serviceInstanceTextAttributesMap);
    }

    private static String convertListToString (List<String> list){
        if (list != null && !list.isEmpty()){
            StringBuilder sb = new StringBuilder();
            for (String item : list) {
                sb.append(item + LIST_STRING_SPLITER);
            }
            // remove the last $
            return sb.toString().substring(0, sb.toString().length()-1);
        }
        return "";
    }


    public static MicroserviceInstance convertToClientMicroserviceInstance(ServerMicroserviceInstance serverMicroserviceInstance) {
        MicroserviceInstance microserviceInstance =  new MicroserviceInstance();
        microserviceInstance.setServiceId(serverMicroserviceInstance.getServiceId());
        microserviceInstance.setInstanceId(serverMicroserviceInstance.getInstanceId());
        microserviceInstance.setHostName(serverMicroserviceInstance.getHostName());
        microserviceInstance.setEndpoints(serverMicroserviceInstance.getEndpoints());
        microserviceInstance.setStatus(MicroserviceInstanceStatus.valueOf(serverMicroserviceInstance.getStatus()));
        return microserviceInstance;
    }

    public static Microservice convertToClientMicroservice(ServerMicroserviceInstance serverMicroserviceInstance) {
        Microservice microservice=  new Microservice();
        microservice.setAppId(serverMicroserviceInstance.getAppId());
        microservice.setServiceId(serverMicroserviceInstance.getServiceId());
        microservice.setServiceName(serverMicroserviceInstance.getServiceName());
        microservice.setVersion(serverMicroserviceInstance.getVersion());
        microservice.setStatus(serverMicroserviceInstance.getStatus());
        microservice.setSchemas(serverMicroserviceInstance.getSchemas());
        return microservice;
    }

    public static String generateServiceId(Microservice microservice){
        String serviceIdStringIndex = String.join("/", microservice.getAppId(), microservice.getServiceName(), microservice.getVersion());
        return UUID.nameUUIDFromBytes(serviceIdStringIndex.getBytes()).toString().split(UUID_SPLITER)[0];
    }

    public static String generateServiceInstanceId(MicroserviceInstance microserviceInstance){
        return UUID.randomUUID().toString().split(UUID_SPLITER)[0];
    }
}
