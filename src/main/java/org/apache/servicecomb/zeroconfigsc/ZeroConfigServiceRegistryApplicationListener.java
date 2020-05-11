package org.apache.servicecomb.zeroconfigsc;

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

import java.util.function.Function;


/**
 * to make sure  ZeroConfigServiceRegistryClientImpl is injected before cse application listener (order is -1000)
 */

@Configuration
@Order(-1001)
public class ZeroConfigServiceRegistryApplicationListener implements ApplicationListener<ApplicationEvent>, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        BeanUtils.setContext(applicationContext);
        ServerUtil.init();

        ServiceRegistryConfig serviceRegistryConfig = ServiceRegistryConfig.INSTANCE;
        Function<ServiceRegistry, ServiceRegistryClient> serviceRegistryClientConstructor =
                serviceRegistry -> new ZeroConfigServiceRegistryClientImpl();
        serviceRegistryConfig.setServiceRegistryClientConstructor(serviceRegistryClientConstructor);
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {}
}
