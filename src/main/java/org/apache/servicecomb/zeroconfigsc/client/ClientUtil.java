package org.apache.servicecomb.zeroconfigsc.client;

import net.posick.mDNS.ServiceInstance;
import net.posick.mDNS.ServiceName;
import org.apache.servicecomb.zeroconfigsc.server.ServerMicroserviceInstance;
import org.apache.servicecomb.foundation.common.net.IpPort;
import org.apache.servicecomb.serviceregistry.api.registry.Microservice;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstance;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstanceStatus;
import org.apache.servicecomb.serviceregistry.client.IpPortManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Name;
import org.xbill.DNS.TextParseException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;

public class ClientUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientUtil.class);

    // serve as the buffer to store current registered microservice itself
    public static Microservice microserviceItSelf = new Microservice();

    public static Optional<ServiceInstance> convertToMDNSServiceInstance(String serviceId, MicroserviceInstance instance, IpPortManager ipPortManager, Microservice microservice) {
        try {

            // have to use service id , as  getMicroservice(serviceId) is based on the id
            ServiceName serviceName = new ServiceName( microservice.getServiceId() + MDNS_SERVICE_NAME_SUFFIX);
            IpPort ipPort = ipPortManager.getAvailableAddress();
            InetAddress[] addresses = new InetAddress[] {InetAddress.getByName(ipPort.getHostOrIp())};

            Map<String, String> serviceInstanceTextAttributesMap = new HashMap<>();
            serviceInstanceTextAttributesMap.put(SERVICE_ID, serviceId);
            serviceInstanceTextAttributesMap.put(INSTANCE_ID, instance.getInstanceId());
            serviceInstanceTextAttributesMap.put(STATUS, instance.getStatus().toString());
            serviceInstanceTextAttributesMap.put(APP_ID, microservice.getAppId());
            serviceInstanceTextAttributesMap.put(SERVICE_NAME, microservice.getServiceName());
            serviceInstanceTextAttributesMap.put(VERSION, microservice.getVersion());

            String hostName = instance.getHostName();
            serviceInstanceTextAttributesMap.put(HOST_NAME, hostName);
            Name mdnsHostName = new Name(hostName + MDNS_HOST_NAME_SUFFIX);

            // use special spliter for schema list otherwise, MDNS can't parse the string list properly i.e.  [schemaId1, schemaId2]
            // schemaId1$schemaId2
            List<String> schemaIdList = microservice.getSchemas();
            StringBuilder schemasSB = new StringBuilder();
            if ( schemaIdList != null && !schemaIdList.isEmpty()) {
                for (String schemaId : schemaIdList) {
                    schemasSB.append(schemaId + SCHEMA_ENDPOINT_LIST_SPLITER);
                }
                // remove the last $
                serviceInstanceTextAttributesMap.put(SCHEMA_IDS,schemasSB.toString().substring(0, schemasSB.toString().length()-1));
            }

            // use special spliter for schema list otherwise, MDNS can't parse the string list properly i.e.  [endpoint1, endpoint1]
            // endpoint1$endpoint2
            List<String> endpoints = instance.getEndpoints();
            StringBuilder endpointsSB = new StringBuilder();
            if ( endpoints != null && !endpoints.isEmpty()) {
                for (String endpoint : endpoints) {
                    endpointsSB.append(endpoint + SCHEMA_ENDPOINT_LIST_SPLITER);
                }
                // remove the last $
                serviceInstanceTextAttributesMap.put(ENDPOINTS,endpointsSB.toString().substring(0, endpointsSB.toString().length()-1));
            }

            return Optional.of(new ServiceInstance(serviceName, 0, 0, ipPort.getPort(), mdnsHostName, addresses, serviceInstanceTextAttributesMap));
        } catch (TextParseException e) {
            LOGGER.error("microservice instance {} has invalid id", instance.getInstanceId(), e);
        } catch (UnknownHostException e1) {
            LOGGER.error("microservice instance {} with Unknown Host name {}/", instance.getInstanceId(), ipPortManager.getAvailableAddress().getHostOrIp(), e1);
        }
        return Optional.empty();
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


    public static Microservice convertMDNSServiceToClientMicroservice(ServiceInstance mdnsService) {
        Microservice microservice = new Microservice();
        Map<String, String> mdnsServiceAttributeMap = mdnsService.getTextAttributes();
        microservice.setAppId(mdnsServiceAttributeMap.get(APP_ID));
        microservice.setServiceId(mdnsServiceAttributeMap.get(SERVICE_ID));
        microservice.setServiceName(mdnsServiceAttributeMap.get(SERVICE_NAME));
        microservice.setVersion(mdnsServiceAttributeMap.get(VERSION));
        microservice.setStatus(mdnsServiceAttributeMap.get(STATUS));

        List<String> schemaIdList = new ArrayList<>();
        String schemaIds = mdnsServiceAttributeMap.get(SCHEMA_IDS);
        if (schemaIds != null) {
            // means only single endpoint
            if (!schemaIds.contains(SCHEMA_ENDPOINT_LIST_SPLITER)){
                schemaIdList.add(schemaIds);
            } else {
                schemaIdList.addAll(Arrays.asList( schemaIds.split("\\$")));
            }
            microservice.setSchemas(schemaIdList);
        }
        return microservice;
    }

    public static MicroserviceInstance convertMDNSServiceToClientMicroserviceInstance(ServiceInstance mdnsServiceInstance) {
        MicroserviceInstance microserviceInstance = new MicroserviceInstance();

        if (mdnsServiceInstance != null && mdnsServiceInstance.getTextAttributes() != null) {
            Map<String, String> mdnsInstanceAttributeMap = mdnsServiceInstance.getTextAttributes();
            microserviceInstance.setServiceId(mdnsInstanceAttributeMap.get(SERVICE_ID));
            microserviceInstance.setInstanceId(mdnsInstanceAttributeMap.get(INSTANCE_ID));
            microserviceInstance.setHostName(mdnsInstanceAttributeMap.get(HOST_NAME));
            microserviceInstance.setStatus(MicroserviceInstanceStatus.valueOf(mdnsInstanceAttributeMap.get(STATUS)));

            List<String> endpointList = new ArrayList<>();
            String endpoints = mdnsInstanceAttributeMap.get(ENDPOINTS);
            if (endpoints != null) {
                // means only single endpoint
                if (!endpoints.contains(SCHEMA_ENDPOINT_LIST_SPLITER)){
                    endpointList.add(endpoints);
                } else {
                    endpointList.addAll(Arrays.asList( endpoints.split("\\$")));
                }
                microserviceInstance.setEndpoints(endpointList);
            }
        }
        return microserviceInstance;
    }

    public static String generateServiceId(String appId, String serviceName){
        String serviceIdStringIndex = String.join("/", appId, serviceName);
        return UUID.nameUUIDFromBytes(serviceIdStringIndex.getBytes()).toString().split(UUID_SPLITER)[0];
    }

    public static String generateServiceInstanceId(MicroserviceInstance microserviceInstance){
        return UUID.randomUUID().toString().split(UUID_SPLITER)[0];
    }


}
