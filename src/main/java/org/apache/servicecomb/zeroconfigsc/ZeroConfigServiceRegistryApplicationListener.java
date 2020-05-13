package org.apache.servicecomb.zeroconfigsc;

import org.apache.servicecomb.zeroconfigsc.client.ClientUtil;
import org.apache.servicecomb.zeroconfigsc.client.ZeroConfigServiceRegistryClientImpl;
import org.apache.servicecomb.foundation.common.utils.BeanUtils;
import org.apache.servicecomb.serviceregistry.ServiceRegistry;
import org.apache.servicecomb.serviceregistry.client.ServiceRegistryClient;
import org.apache.servicecomb.serviceregistry.config.ServiceRegistryConfig;
import org.apache.servicecomb.zeroconfigsc.server.ServerUtil;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.util.StringUtils;

import java.util.function.Function;


/**
 * ZeroConfigServiceRegistryClientImpl is injected before cse application listener (order is -1000)
 */

@Configuration
@Order(-1001)
public class ZeroConfigServiceRegistryApplicationListener implements ApplicationListener<ApplicationEvent>, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        // Same mechanims as Local registry to enable the Zero Config registry
        String flag = System.getProperty(ZeroConfigRegistryConstants.ZERO_CONFIG_REGISTRY_FLAG);
        if (!StringUtils.isEmpty(flag) && flag.equals(ZeroConfigRegistryConstants.ZERO_CONFIG_REGISTRY_ENABLE_FLAG)){
            this.applicationContext = applicationContext;
            BeanUtils.setContext(applicationContext);
            ServerUtil.init();
            ClientUtil.init();

            ServiceRegistryConfig serviceRegistryConfig = ServiceRegistryConfig.INSTANCE;
            Function<ServiceRegistry, ServiceRegistryClient> serviceRegistryClientConstructor =
                serviceRegistry -> new ZeroConfigServiceRegistryClientImpl();
            serviceRegistryConfig.setServiceRegistryClientConstructor(serviceRegistryClientConstructor);
        }
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {}
}
