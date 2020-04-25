package org.apache.servicecomb.zeroconfigsc;


import org.apache.servicecomb.zeroconfigsc.server.ServerMicroserviceInstance;
import org.apache.servicecomb.zeroconfigsc.server.ZeroConfigRegistryServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;


/**
 * facade class for consumer to discover the service provider
 */

public class ZeroConfigServiceRegistryFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroConfigServiceRegistryFacade.class);

    public static List<String> discover(String serviceName) {
        Map<String, List<ServerMicroserviceInstance>> map = ZeroConfigRegistryServerUtil.getserverMicroserviceInstanceMapByServiceName();
        if (map.containsKey(serviceName)){
            List<ServerMicroserviceInstance> list = map.get(serviceName);
            if (list != null && !list.isEmpty()){
                return list.get(0).getEndpoints();
            }
        }
        return null;
    }

}
