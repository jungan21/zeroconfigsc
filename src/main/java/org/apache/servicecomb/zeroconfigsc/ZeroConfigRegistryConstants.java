package org.apache.servicecomb.zeroconfigsc;

public interface ZeroConfigRegistryConstants {

    // MulticastSocket related
    String GROUP = "225.0.0.0";
    Integer PORT = 6666;
    String EVENT = "event";
    String REGISTER_EVENT = "register";
    String UNREGISTER_EVENT = "unregister";
    String HEARTBEAT_EVENT = "heartbeat";

    // Microservice Attributes
    String APP_ID = "appId";
    String SERVICE_NAME = "serviceName";
    String VERSION = "version";
    String SERVICE_ID = "serviceId";
    String STATUS = "status";
    String SCHEMA_IDS = "schemas";

    // Microservice Instance Attributes
    String INSTANCE_ID = "instanceId";
    String ENDPOINTS = "endpoints";
    String HOST_NAME = "hostName";
    String INSTANCE_HEARTBEAT_RESPONSE_MESSAGE_OK = "OK";


    // Schema Content Path related
    String SCHEMA_CONTENT_ENDPOINT = "schemaContentEndpoint";
    String SCHEMA_CONTENT_ENDPOINT_BASE_PATH = "/schemaEndpoint";
    String SCHEMA_CONTENT_ENDPOINT_SUBPATH = "/schemas";
    String SCHEMA_CONTENT_ENDPOINT_QUERY_KEYWORD= "schemaId";

    //others
    String LIST_STRING_SPLITER = "$";
    String UUID_SPLITER = "-";
    long HEALTH_CHECK_INTERVAL = 3;
    String ENDPOINT_PREFIX_REST = "rest";
    String ENDPOINT_PREFIX_HTTP = "http";
}
