package minibroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class LogRecoveryTest {

    private static void writeThreeRecords(Path file) throws Exception {
        try (Log log = new Log(file)) {
            log.append(new Record("k0".getBytes(UTF_8), "v0".getBytes(UTF_8), 1000));
            log.append(new Record(null,                "v1".getBytes(UTF_8), 1001));
            log.append(new Record("k2".getBytes(UTF_8), "v2".getBytes(UTF_8), 1002));
        }
    }

    private static void assertThreeRecords(Log log) throws Exception {
        assertEquals(3, log.endOffset());
        List<StoredRecord> recs = log.read(0, 1 << 20);
        assertEquals(3, recs.size());
        assertArrayEquals("v0".getBytes(UTF_8), recs.get(0).value());
        assertNull(recs.get(1).key());
        assertArrayEquals("v1".getBytes(UTF_8), recs.get(1).value());
        assertEquals(2, recs.get(2).offset());
        assertArrayEquals("v2".getBytes(UTF_8), recs.get(2).value());
    }

    @Test
    void reopenRecoversDurableRecords(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("partition.log");
        writeThreeRecords(file);

        // Brand new Log over the same file: state must be rebuilt purely from disk.
        try (Log log = new Log(file)) {
            assertThreeRecords(log);
        }
    }

    @Test
    void corruptTailIsTruncatedOnReopen(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("partition.log");
        writeThreeRecords(file);

        // Simulate a half-written 4th record: append garbage bytes at EOF.
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0, 0, 0, 50, 1, 2, 3}), ch.size());
        }

        // Recovery must drop the garbage tail and keep the 3 intact records.
        try (Log log = new Log(file)) {
            assertThreeRecords(log);
        }

        // And the file must be physically truncated (garbage really gone): a second
        // reopen sees the same clean state and can append at offset 3.
        try (Log log = new Log(file)) {
            assertThreeRecords(log);
            assertEquals(3, log.append(new Record("k3".getBytes(UTF_8), "v3".getBytes(UTF_8), 1003)));
            assertEquals(4, log.endOffset());
        }
    }
}
