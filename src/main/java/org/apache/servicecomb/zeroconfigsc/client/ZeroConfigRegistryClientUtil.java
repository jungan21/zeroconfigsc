package org.apache.servicecomb.zeroconfigsc.client;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.APP_ID;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SERVICE_ID;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SERVICE_NAME;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.VERSION;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.STATUS;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.ENDPOINTS;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.HOST_NAME;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.INSTANCE_ID;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SCHEMA_ENDPOINT_LIST_SPLITER;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.UUID_SPLITER;

import org.apache.servicecomb.zeroconfigsc.server.ServerMicroserviceInstance;
import org.apache.servicecomb.foundation.common.net.IpPort;
import org.apache.servicecomb.serviceregistry.api.registry.Microservice;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstance;
import org.apache.servicecomb.serviceregistry.api.registry.MicroserviceInstanceStatus;
import org.apache.servicecomb.serviceregistry.client.IpPortManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Name;
import java.util.*;

public class ZeroConfigRegistryClientUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigRegistryClientUtil.class);

    public static Optional<Map<String, String>> convertToMDNSServiceInstance(String serviceId, String microserviceInstanceId, MicroserviceInstance microserviceInstance, IpPortManager ipPortManager, Microservice microservice) {
        Map<String, String> serviceInstanceTextAttributesMap = new HashMap<>();
        serviceInstanceTextAttributesMap.put(SERVICE_ID, serviceId);
        serviceInstanceTextAttributesMap.put(INSTANCE_ID, microserviceInstanceId);
        serviceInstanceTextAttributesMap.put(STATUS, microserviceInstance.getStatus().toString());
        serviceInstanceTextAttributesMap.put(APP_ID, microservice.getAppId());
        serviceInstanceTextAttributesMap.put(SERVICE_NAME, microservice.getServiceName());
        serviceInstanceTextAttributesMap.put(VERSION, microservice.getVersion());

        String hostName = microserviceInstance.getHostName();
        serviceInstanceTextAttributesMap.put(HOST_NAME, hostName);
        Name mdnsHostName = new Name(hostName + MDNS_HOST_NAME_SUFFIX);


        // use special spliter for schema list otherwise, MDNS can't parse the string list properly i.e.  [schema1, schema2]
        // schema1$schema2
        List<String> endpoints = microserviceInstance.getEndpoints();
        StringBuilder sb = new StringBuilder();
        if ( endpoints != null && !endpoints.isEmpty()) {
            for (String endpoint : endpoints) {
                sb.append(endpoint + SCHEMA_ENDPOINT_LIST_SPLITER);
            }
            // remove the last $
            serviceInstanceTextAttributesMap.put(ENDPOINTS,sb.toString().substring(0, sb.toString().length()-1));
        }
        return Optional.of(serviceInstanceTextAttributesMap);
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

    public static String generateServiceId(Microservice microservice){
        String serviceIdStringIndex = String.join("/", microservice.getAppId(), microservice.getServiceName(), microservice.getVersion());
        return UUID.nameUUIDFromBytes(serviceIdStringIndex.getBytes()).toString().split(UUID_SPLITER)[0];
    }

    public static String generateServiceInstanceId(MicroserviceInstance microserviceInstance){
        return UUID.randomUUID().toString().split(UUID_SPLITER)[0];
    }
}
