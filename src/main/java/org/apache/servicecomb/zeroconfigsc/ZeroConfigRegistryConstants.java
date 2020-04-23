package org.apache.servicecomb.zeroconfigsc;

public interface ZeroConfigRegistryConstants {

    // MulticastSocket related
    String GROUP  = "225.0.0.0";
    Integer PORT = 6666;

    // Microservice Attributes
    String APP_ID = "appId";
    String SERVICE_NAME = "serviceName";
    String VERSION = "version";
    String SERVICE_ID = "serviceId";
    String STATUS = "status";

    // Microservice Instance Attributes
    String INSTANCE_ID = "instanceId";
    String ENDPOINTS = "endpoints";
    String HOST_NAME = "hostName";
    String INSTANCE_HEARTBEAT_RESPONSE_MESSAGE_OK = "OK";
    String ENDPOINT_PREFIX_REST = "rest";
    String ENDPOINT_PREFIX_HTTP = "http";

    //others
    String SPLITER_MAP_KEY_VALUE = "=";
    String SCHEMA_ENDPOINT_LIST_SPLITER = "$";
    String UUID_SPLITER = "-";
}
