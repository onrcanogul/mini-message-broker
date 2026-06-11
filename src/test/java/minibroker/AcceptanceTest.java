package minibroker;

import minibroker.broker.BrokerServer;
import minibroker.broker.Config;
import minibroker.broker.LogManager;
import minibroker.broker.RequestHandler;
import minibroker.client.BrokerClient;
import minibroker.storage.Record;
import minibroker.storage.StoredRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

/** End-to-end v0 Definition of Done: when this passes, v0 is finished. */
class AcceptanceTest {

    private static Record r(String key, String value) {
        return new Record(key == null ? null : key.getBytes(UTF_8), value.getBytes(UTF_8), 0);
    }

    /** Starts a fresh server over the given dataDir. */
    private static BrokerServer startServer(Path dataDir) throws IOException {
        BrokerServer server = new BrokerServer(
                new Config(0, dataDir),
                new RequestHandler(new LogManager(dataDir)));
        server.start();
        return server;
    }

    @Test
    void v0AcceptanceCriteria(@TempDir Path dataDir) throws Exception {
        // 1) Server starts on a free port over a temp dataDir.
        BrokerServer server = startServer(dataDir);
        assertTrue(server.port() > 0, "server bound to a real port");
        assertNotNull(dataDir);

        try (BrokerClient client = BrokerClient.connect("localhost", server.port())) {
            // 2) First produce -> baseOffset 0.
            long base1 = client.produce("orders", List.of(r("k1", "hello"), r("k2", "world")));
            assertEquals(0, base1);

            // 3) Second produce -> baseOffset 2 (continues the log).
            long base2 = client.produce("orders", List.of(r("k3", "again")));
            assertEquals(2, base2);

            // 4) Fetch all -> 3 records, offsets 0,1,2, HW 3, contents correct.
            BrokerClient.FetchResult res = client.fetch("orders", 0, 1 << 20);
            assertEquals(3, res.highWatermark());
            List<StoredRecord> recs = res.records();
            assertEquals(3, recs.size());
            assertEquals(0, recs.get(0).offset());
            assertArrayEquals("k1".getBytes(UTF_8), recs.get(0).key());
            assertArrayEquals("hello".getBytes(UTF_8), recs.get(0).value());
            assertEquals(1, recs.get(1).offset());
            assertArrayEquals("world".getBytes(UTF_8), recs.get(1).value());
            assertEquals(2, recs.get(2).offset());
            assertArrayEquals("again".getBytes(UTF_8), recs.get(2).value());
        }

        // 5) DURABILITY: stop the broker, start a NEW one over the SAME dataDir (recovery).
        server.close();
        BrokerServer restarted = startServer(dataDir);
        try (BrokerClient client = BrokerClient.connect("localhost", restarted.port())) {
            BrokerClient.FetchResult res = client.fetch("orders", 0, 1 << 20);
            assertEquals(3, res.highWatermark(), "recovered HW");
            List<StoredRecord> recs = res.records();
            assertEquals(3, recs.size(), "all 3 records survived the restart");
            assertArrayEquals("hello".getBytes(UTF_8), recs.get(0).value());
            assertArrayEquals("world".getBytes(UTF_8), recs.get(1).value());
            assertArrayEquals("again".getBytes(UTF_8), recs.get(2).value());

            // 6) Fetch at the end of the log -> empty list, HW still 3 (caught up, not an error).
            BrokerClient.FetchResult tail = client.fetch("orders", 3, 1 << 20);
            assertEquals(3, tail.highWatermark());
            assertTrue(tail.records().isEmpty(), "no data beyond the log end");
        } finally {
            restarted.close();
        }
    }
}
