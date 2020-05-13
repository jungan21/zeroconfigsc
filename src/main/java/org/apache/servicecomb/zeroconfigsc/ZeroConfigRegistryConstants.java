package org.apache.servicecomb.zeroconfigsc;

public interface ZeroConfigRegistryConstants {

    String ZERO_CONFIG_REGISTRY_FLAG = "zeroconfig.registry.flag";
    String ZERO_CONFIG_REGISTRY_ENABLE_FLAG = "enable";

    // MulticastSocket related
    String GROUP = "225.0.0.0";
    Integer PORT = 6666;
    Integer TIME_TO_LIVE = 255;
    Integer DATA_PACKET_BUFFER_SIZE = 2048; // 2K
    long HEALTH_CHECK_INTERVAL = 3;
    long CLIENT_DELAY = 2;
    long SERVER_DELAY = 5;

    // Event
    String EVENT = "event";
    String REGISTER_EVENT = "register";
    String UNREGISTER_EVENT = "unregister";
    String HEARTBEAT_EVENT = "heartbeat";

    // Microservice & Instance Attributes
    String APP_ID = "appId";
    String SERVICE_NAME = "serviceName";
    String VERSION = "version";
    String SERVICE_ID = "serviceId";
    String STATUS = "status";
    String SCHEMA_IDS = "schemas";
    String INSTANCE_ID = "instanceId";
    String ENDPOINTS = "endpoints";
    String HOST_NAME = "hostName";
    String INSTANCE_HEARTBEAT_RESPONSE_MESSAGE_OK = "OK";

    // Schema Content Endpoint and Path related
    String SCHEMA_CONTENT_ENDPOINT = "schemaContentEndpoint";
    String SCHEMA_CONTENT_ENDPOINT_BASE_PATH = "/schemaEndpoint";
    String SCHEMA_CONTENT_ENDPOINT_SUBPATH = "/schemas";
    String SCHEMA_CONTENT_ENDPOINT_QUERY_KEYWORD= "schemaId";

    // others
    String MAP_STRING_LEFT = "{";
    String MAP_STRING_RIGHT = "}";
    String MAP_ELEMENT_SPILITER = ",";
    String MAP_KV_SPILITER = "=";
    String LIST_STRING_SPLITER = "$";
    String UUID_SPLITER = "-";
    String SERVICE_ID_SPLITER = "/";
    String ENDPOINT_PREFIX_REST = "rest";
    String ENDPOINT_PREFIX_HTTP = "http";
}
