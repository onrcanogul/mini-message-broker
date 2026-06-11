package minibroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class LogTest {

    @Test
    void appendAssignsOffsetsThenReadBack(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("partition.log");
        try (Log log = new Log(file)) {
            // 3 records: the second one has a null key (tombstone-like / keyless message).
            assertEquals(0, log.append(new Record("k0".getBytes(UTF_8), "v0".getBytes(UTF_8), 1000)));
            assertEquals(1, log.append(new Record(null,                "v1".getBytes(UTF_8), 1001)));
            assertEquals(2, log.append(new Record("k2".getBytes(UTF_8), "v2".getBytes(UTF_8), 1002)));
            assertEquals(3, log.endOffset(), "LEO should be 3");

            List<StoredRecord> recs = log.read(0, 1 << 20); // large maxBytes -> all three come back
            assertEquals(3, recs.size());

            assertEquals(0, recs.get(0).offset());
            assertArrayEquals("k0".getBytes(UTF_8), recs.get(0).key());
            assertArrayEquals("v0".getBytes(UTF_8), recs.get(0).value());
            assertEquals(1000, recs.get(0).timestamp());

            assertEquals(1, recs.get(1).offset());
            assertNull(recs.get(1).key(), "null key must be preserved");
            assertArrayEquals("v1".getBytes(UTF_8), recs.get(1).value());

            assertEquals(2, recs.get(2).offset());
            assertArrayEquals("k2".getBytes(UTF_8), recs.get(2).key());
            assertArrayEquals("v2".getBytes(UTF_8), recs.get(2).value());
        }
    }
}
