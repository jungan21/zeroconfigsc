package org.apache.servicecomb.zeroconfigsc.server;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SCHEMA_ENDPOINT_LIST_SPLITER;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ZeroConfigRegistryServerUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigRegistryServerUtil.class);

    private static MulticastSocket multicastSocket;

    private static ZeroConfigRegistryService zeroConfigRegistryService;

    private static InetAddress group;

    // 1st key: serviceId, 2nd key: instanceId
    private static Map<String, Map<String, ServerMicroserviceInstance>>  serverMicroserviceInstanceMap = new ConcurrentHashMap<>();

    // 1st key: serviceName, 2nd key: Version
    private static Map<String, List<ServerMicroserviceInstance>>  serverMicroserviceInstanceMapByServiceName = new ConcurrentHashMap<>();

    public static Map<String, Map<String, ServerMicroserviceInstance>>  getServerMicroserviceInstanceMap() {
        return serverMicroserviceInstanceMap;
    }

    public static Map<String, List<ServerMicroserviceInstance>>  getserverMicroserviceInstanceMapByServiceName() {
        return serverMicroserviceInstanceMapByServiceName;
    }

    public static synchronized void init() {
        zeroConfigRegistryService = new ZeroConfigRegistryService();
        try {
            group = InetAddress.getByName(GROUP);
        } catch (UnknownHostException e) {
            LOGGER.error("Unknow host exception when creating goup" + e);
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            startListenerForRegisterUnregisterEvent();
        });

    }

    public static Optional<ServerMicroserviceInstance> convertToServerMicroserviceInstance(Map<String, String> serviceAttributeMap){
        return  Optional.of(buildServerMicroserviceInstanceFromMap(serviceAttributeMap));
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

        // rest://127.0.0.1:8080$rest://127.0.0.1:8081
        String endPointsString = serviceAttributeMap.get(ENDPOINTS);
        if ( endPointsString != null && !endPointsString.isEmpty()){
            if (endPointsString.contains(SCHEMA_ENDPOINT_LIST_SPLITER)){
                serverMicroserviceInstance.setEndpoints(Arrays.asList(endPointsString.split("\\$")));
            } else {
                List<String> list  = new ArrayList<>();
                list.add(endPointsString);
                serverMicroserviceInstance.setEndpoints(list);
            }
        }
        return serverMicroserviceInstance;
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
            byte[] buffer = new byte[4096]; // 4k
            multicastSocket =  new MulticastSocket(PORT);
            group = InetAddress.getByName(GROUP);
            multicastSocket.joinGroup(group); // need to join the group to be able to receive the data

            while (true) {
                DatagramPacket receivePacketBuffer = new DatagramPacket(buffer, buffer.length);
                multicastSocket.receive(receivePacketBuffer);
                String receivedPacketString = new String(receivePacketBuffer.getData());

                /**
                 * TODO Received service register/unregister event: {}{hostName=DESKTOP-Q2K46AO, instanceId=e3f169d7, appId=springmvc-sample, event=register,
                 *  serviceId=16e8633d, serviceName=springmvcConsumer, version=0.0.2, status=UP}version=0.0.2, status=UP}
                 */
                LOGGER.info("Received service register/unregister event: {}", receivedPacketString);
                Map<String, String> receivedStringMap = getMapFromString(receivedPacketString);
                LOGGER.info("Converted service register/unregister event to Map {}", receivedStringMap);
                if ( receivedStringMap != null && receivedStringMap.containsKey(EVENT)){

                    String event = receivedStringMap.get(EVENT);
                    if (event.equals(REGISTER_EVENT)){
                        zeroConfigRegistryService.registerMicroserviceInstance(receivedStringMap);
                    } else if (event.equals(UNREGISTER_EVENT)) {
                        zeroConfigRegistryService.unregisterMicroserviceInstance(receivedStringMap);
                    } else {
                        LOGGER.error("Unrecognized event type. event: {}" + event);
                    }
                } else {
                    LOGGER.error("Received event is null or doesn't have event type. {}" + receivedPacketString);
                }
            }

        } catch (IOException e) {
            //failed to create MulticastSocket, the PORT might have been occupied
            LOGGER.error("Received service register/unregister event" + e);
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
