package org.apache.servicecomb.zeroconfigsc.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ServerUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerUtil.class);

    private static MulticastSocket multicastSocket;

    private static ZeroConfigRegistryService zeroConfigRegistryService;

    private static InetAddress group;

    private static ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    // 1st key: serviceId, 2nd key: instanceId
    public static Map<String, Map<String, ServerMicroserviceInstance>>  microserviceInstanceMap = new ConcurrentHashMap<>();

    public static synchronized void init() {
        zeroConfigRegistryService = new ZeroConfigRegistryService();
        try {
            group = InetAddress.getByName(GROUP);
        } catch (UnknownHostException e) {
            LOGGER.error("Unknown host exception when creating group" + e);
        }
        startEventListenerThread();
        startInstanceStatusCheckerThread();
    }

    private static void startEventListenerThread(){
        ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
        listenerExecutor.submit(() -> {
            startListenerForRegisterUnregisterEvent();
        });
    }

    private static void startInstanceStatusCheckerThread(){
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                LOGGER.info("Server side runs scheduled health check task");
                List<ServerMicroserviceInstance> toBeRemovedInstanceList = new ArrayList<>();

                try {
                    // check status
                    for (Map.Entry<String, Map<String, ServerMicroserviceInstance>> entry : microserviceInstanceMap.entrySet()){
                        if (entry.getValue() != null){
                            Map<String, ServerMicroserviceInstance>  instanceIdMap = entry.getValue();
                            for (Map.Entry<String, ServerMicroserviceInstance> innerEntry : instanceIdMap.entrySet()) {
                                ServerMicroserviceInstance instance = innerEntry.getValue();
                                // current time - last heartbeattime > 3 seconds => dead instance
                                if (instance.getLastHeartbeatTimeStamp() != null &&
                                        instance.getLastHeartbeatTimeStamp().plusSeconds(HEALTH_CHECK_INTERVAL).compareTo(Instant.now()) < 0)
                                    toBeRemovedInstanceList.add(innerEntry.getValue());
                            }
                        }
                    }

                    // remove dead instances
                    if (!toBeRemovedInstanceList.isEmpty()){
                        for (ServerMicroserviceInstance instance : toBeRemovedInstanceList){
                            for (Map.Entry<String, Map<String, ServerMicroserviceInstance>> entry : microserviceInstanceMap.entrySet()){
                                String serviceId = entry.getKey();
                                Map<String, ServerMicroserviceInstance>  instanceIdMap = entry.getValue();
                                if (serviceId.equals(instance.getServiceId())){
                                    instanceIdMap.remove(instance.getServiceId());
                                }
                            }
                        }
                    }

                    LOGGER.info("JUNGAN DEBUG microserviceInstanceMap after remove dead instance:  {}", microserviceInstanceMap);

                } catch (Exception e) {
                    LOGGER.error("Failed to start instance status checker thread", e);
                }

            }
        };

        scheduledExecutor.scheduleAtFixedRate(runnable, 5, 3, TimeUnit.SECONDS);

    }

    public static Optional<ServerMicroserviceInstance> convertToServerMicroserviceInstance(Map<String, String> serviceInstanceAttributeMap){
        return  Optional.of(buildServerMicroserviceInstanceFromMap(serviceInstanceAttributeMap));
    }

    private static ServerMicroserviceInstance buildServerMicroserviceInstanceFromMap (Map<String, String> serviceAttributeMap) {
        ServerMicroserviceInstance serverMicroserviceInstance = new ServerMicroserviceInstance();
        serverMicroserviceInstance.setInstanceId(serviceAttributeMap.get(INSTANCE_ID));
        serverMicroserviceInstance.setServiceId(serviceAttributeMap.get(SERVICE_ID));
        serverMicroserviceInstance.setStatus(serviceAttributeMap.get(STATUS));
        serverMicroserviceInstance.setHostName(serviceAttributeMap.get(HOST_NAME));
        serverMicroserviceInstance.setAppId(serviceAttributeMap.get(APP_ID));
        serverMicroserviceInstance.setServiceName(serviceAttributeMap.get(SERVICE_NAME));
        serverMicroserviceInstance.setVersion(serviceAttributeMap.get(VERSION));
        // list type attributes
        serverMicroserviceInstance.setEndpoints(convertStringToList(serviceAttributeMap.get(ENDPOINTS)));
        serverMicroserviceInstance.setSchemas(convertStringToList(serviceAttributeMap.get(SCHEMA_IDS)));
        return serverMicroserviceInstance;
    }

    // rest://127.0.0.1:8080$rest://127.0.0.1:8081
    // schemaId1$schemaId2
    private static List<String> convertStringToList(String listString){
        List<String> resultList  = new ArrayList<>();
        if (listString != null && !listString.isEmpty()){
            if (listString.contains(LIST_STRING_SPLITER)){
                resultList = Arrays.asList(listString.split("\\$"));
            } else {
                resultList.add(listString);
            }
        }
        return resultList;
    }

    private static Map<String, String> getMapFromString(String str){
        Map<String,String> map = new HashMap<>();
        String trimedString = str.trim();
        if (trimedString.startsWith("{") && trimedString.endsWith("}") && trimedString.length() > 2) {
            trimedString = trimedString.substring(1, trimedString.length()-1);
            String[] keyValue = trimedString.split(",");
            for (int i = 0; i < keyValue.length; i++) {
                String[] str2 = keyValue[i].split("=");
                if(str2.length-1 == 0){
                    map.put(str2[0].trim(),"");
                }else{
                    map.put(str2[0].trim(),str2[1].trim());
                }
            }
        } else {
            LOGGER.error("Wrong format of the input received string: {}", trimedString);
        }
        return map;
    }

    private static void startListenerForRegisterUnregisterEvent () {
        try {
            byte[] buffer = new byte[2048]; // 2k
            multicastSocket =  new MulticastSocket(PORT);
            group = InetAddress.getByName(GROUP);
            multicastSocket.joinGroup(group); // need to join the group to be able to receive the data

            while (true) {
                DatagramPacket receivePacketBuffer = new DatagramPacket(buffer, buffer.length);
                multicastSocket.receive(receivePacketBuffer);
                String receivedPacketString = new String(receivePacketBuffer.getData());

                Map<String, String> receivedStringMap = getMapFromString(receivedPacketString);

                if ( receivedStringMap != null && receivedStringMap.containsKey(EVENT)){
                    String event = receivedStringMap.get(EVENT);
                    if (event.equals(REGISTER_EVENT)){
                        LOGGER.info("Received service register event{}", receivedStringMap);
                        zeroConfigRegistryService.registerMicroserviceInstance(receivedStringMap);
                    } else if (event.equals(UNREGISTER_EVENT)) {
                        LOGGER.info("Received service unregister event{}", receivedStringMap);
                        zeroConfigRegistryService.unregisterMicroserviceInstance(receivedStringMap);
                    } else if (event.equals(HEARTBEAT_EVENT)) {
                        LOGGER.info("Received service heartbeat event{}", receivedStringMap);
                        zeroConfigRegistryService.heartbeat(receivedStringMap);
                    } else {
                        LOGGER.error("Unrecognized event type. event: {}", event);
                    }
                } else {
                    LOGGER.error("Received service event is null or doesn't have event type. {}", receivedPacketString);
                }
                LOGGER.info("JUNGAN DEBUG microserviceInstanceMap: {} after receiving new event", ServerUtil.microserviceInstanceMap);
            }

        } catch (IOException e) {
            //failed to create MulticastSocket, the PORT might have been occupied
            LOGGER.error("Failed to create MulticastSocket object for receiving register/unregister event" + e);
        } finally {
            if (multicastSocket != null) {
                try {
                    multicastSocket.leaveGroup(group);
                    multicastSocket.close();
                } catch (IOException e1) {
                    //  如果没有加入group不会报错，但是如果group不是组播地址将报错
                    LOGGER.error("Failed to close the MulticastSocket" + e1);
                }
            }
        }
    }
}
