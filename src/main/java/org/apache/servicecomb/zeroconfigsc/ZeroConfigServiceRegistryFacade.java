//package org.apache.servicecomb.zeroconfigsc;
//
//import net.posick.mDNS.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import java.io.IOException;
//import java.util.*;
//
//
///**
// * facade class for consumer to discover the service provider
// */
//import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.*;
//
//public class ZeroConfigServiceRegistryFacade {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigServiceRegistryFacade.class);
//
//    /**
//     * @param serviceName
//     * @return service endpoint
//     * @throws IOException
//     */
//    public static String discover(String serviceName) throws IOException {
//        String endpoint = null;
//        ServiceName mdnsServiceName  = new ServiceName(serviceName + MDNS_SERVICE_NAME_SUFFIX);
//        Lookup lookup = null;
//        try {
//            lookup = new Lookup(mdnsServiceName);
//            ServiceInstance[] services = lookup.lookupServices();
//            for (ServiceInstance service : services) {
//                Map<String, String> attributesMap = service.getTextAttributes();
//                if (attributesMap != null && attributesMap.containsKey(ENDPOINTS)) {
//                    String tempEndpoint = attributesMap.get(ENDPOINTS);
//                    if (!tempEndpoint.contains(SCHEMA_ENDPOINT_LIST_SPLITER)){
//                        endpoint = tempEndpoint.replace(ENDPOINT_PREFIX_REST, ENDPOINT_PREFIX_HTTP);
//                    } else {
//                        endpoint = tempEndpoint.split("\\$")[0].replace(ENDPOINT_PREFIX_REST, ENDPOINT_PREFIX_HTTP);
//                    }
//                }
//            }
//        } finally {
//            if (lookup != null) {
//                try {
//                    lookup.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return endpoint;
//    }
//
//}
