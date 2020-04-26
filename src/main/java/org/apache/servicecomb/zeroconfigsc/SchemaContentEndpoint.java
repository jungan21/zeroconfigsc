package org.apache.servicecomb.zeroconfigsc;

import org.apache.servicecomb.provider.rest.common.RestSchema;
import org.apache.servicecomb.zeroconfigsc.client.ClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SCHEMA_CONTENT_ENDPOINT;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SCHEMA_CONTENT_ENDPOINT_BASE_PATH;

import javax.ws.rs.core.MediaType;

import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SCHEMA_CONTENT_ENDPOINT_QUERY_KEYWORD;
import static org.apache.servicecomb.zeroconfigsc.ZeroConfigRegistryConstants.SCHEMA_CONTENT_ENDPOINT_SUBPATH;

@RestSchema(schemaId = SCHEMA_CONTENT_ENDPOINT)
@RequestMapping(path = SCHEMA_CONTENT_ENDPOINT_BASE_PATH)
public class SchemaContentEndpoint {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaContentEndpoint.class);

    // each service expose its endpoint for others(consumers) to retrieve its schema content
    @RequestMapping(path = SCHEMA_CONTENT_ENDPOINT_SUBPATH, produces = MediaType.APPLICATION_JSON, method = RequestMethod.POST )
    public String getSchemaEndpoint(@RequestParam(name = SCHEMA_CONTENT_ENDPOINT_QUERY_KEYWORD) String schemaId) {
        LOGGER.info("Received call from consumer to retrieve the schema content for SchemaId: {}", schemaId);
        if (ClientUtil.microserviceItSelf.getSchemaMap().containsKey(schemaId)) {
            return ClientUtil.microserviceItSelf.getSchemaMap().get(schemaId);
        } else {
            LOGGER.error("schemaId: {} doesn't existing", schemaId);
            return "";
        }
        // return ClientUtil.microserviceItSelf.getSchemaMap().computeIfPresent(schemaId,  (k, v) -> { return v; });
    }
}
