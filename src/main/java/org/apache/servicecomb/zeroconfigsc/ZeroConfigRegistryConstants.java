package org.apache.servicecomb.zeroconfigsc;

public interface ZeroConfigRegistryConstants {

    // MDNS Related
    String MDNS_SERVICE_NAME_SUFFIX = "._http._tcp.local.";
    String MDNS_HOST_NAME_SUFFIX = ".local.";
    String[] DISCOVER_SERVICE_TYPES = new String[]
            {
                    "_http._tcp.",              // Web pages
                    "_printer._sub._http._tcp", // Printer configuration web pages
                    "_org.smpte.st2071.device:device_v1.0._sub._mdc._tcp",  // SMPTE ST2071 Devices
                    "_org.smpte.st2071.service:service_v1.0._sub._mdc._tcp"  // SMPTE ST2071 Services
            };

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
    String SCHEMA_ENDPOINT_LIST_SPLITER = "$";
    String UUID_SPLITER = "-";
    String ENDPOINT_PREFIX_REST = "rest";
    String ENDPOINT_PREFIX_HTTP = "http";
}
