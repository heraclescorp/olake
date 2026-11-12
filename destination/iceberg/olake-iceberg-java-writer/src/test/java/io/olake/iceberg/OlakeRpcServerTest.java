package io.olake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class OlakeRpcServerTest {
    @Test
    void ordinarySyncCatalogTransportIsUnchanged() {
        Map<String, String> properties = new HashMap<>(Map.of("rest.client.socket-timeout-ms", "0"));
        OlakeRpcServer.configureResyncCatalog(properties);
        assertEquals(Map.of("rest.client.socket-timeout-ms", "0"), properties);
    }

    @Test
    void freshResyncHasBoundedCatalogTransportBeforeInitialization() {
        Map<String, String> properties = new HashMap<>(Map.of("atomic-resync-enabled", "true"));
        OlakeRpcServer.configureResyncCatalog(properties);
        assertEquals(Map.of("rest.client.connection-timeout-ms", "30000", "rest.client.socket-timeout-ms", "60000",
                "rest.client.max-retries", "5"), properties);
    }

    @Test
    void resyncClampsUnboundedSettingsButKeepsTighterOperatorLimits() {
        Map<String, String> properties = new HashMap<>(Map.of("atomic-resync-enabled", "true",
                "rest.client.connection-timeout-ms", "1000", "rest.client.socket-timeout-ms", "0",
                "rest.client.max-retries", "100"));
        OlakeRpcServer.configureResyncCatalog(properties);
        assertFalse(properties.containsKey("atomic-resync-enabled"));
        assertEquals("1000", properties.get("rest.client.connection-timeout-ms"));
        assertEquals("60000", properties.get("rest.client.socket-timeout-ms"));
        assertEquals("5", properties.get("rest.client.max-retries"));
    }
}
