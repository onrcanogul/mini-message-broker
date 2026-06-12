package minibroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class LogRollingTest {

    private static Record rec(int i) {
        return new Record(("k" + i).getBytes(UTF_8), ("v" + i).getBytes(UTF_8), 1000 + i);
    }

    /** On-disk size of one record (recLen + body + crc) — mirrors the segment layout. */
    private static long diskSize(byte[] key, byte[] value) {
        int keyLen = key == null ? 0 : key.length;
        int body = 8 + 8 + 4 + keyLen + 4 + value.length;
        return 4 + body + 4;
    }

    private static List<String> logFileNames(Path dir) throws Exception {
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".log"))
                    .sorted()
                    .toList();
        }
    }

    @Test
    void rollsAndReadsAcrossSegments(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("partition.log");
        // Small enough to hold ~2 of these (uniform-size) records per segment.
        long segBytes = 2 * diskSize("k0".getBytes(UTF_8), "v0".getBytes(UTF_8));

        try (Log log = new Log(file, segBytes)) {
            for (int i = 0; i < 5; i++) {
                assertEquals(i, log.append(rec(i)), "offsets are contiguous across segments");
            }
            assertEquals(5, log.endOffset());

            // More than one segment file, with correct base offsets (2 per segment: 0,2,4).
            List<String> files = logFileNames(dir);
            assertTrue(files.size() > 1, "rolling should create multiple segments");
            assertEquals(List.of(
                    "00000000000000000000.log",
                    "00000000000000000002.log",
                    "00000000000000000004.log"), files);

            // read(0): all 5 records, in order, crossing segment boundaries.
            List<StoredRecord> all = log.read(0, 1 << 20);
            assertEquals(5, all.size());
            for (int i = 0; i < 5; i++) {
                assertEquals(i, all.get(i).offset());
                assertArrayEquals(("v" + i).getBytes(UTF_8), all.get(i).value());
            }

            // read(3): starts mid-segment (base 2 holds 2,3) and spans into segment base 4.
            List<StoredRecord> tail = log.read(3, 1 << 20);
            assertEquals(2, tail.size());
            assertEquals(3, tail.get(0).offset());
            assertEquals(4, tail.get(1).offset());
        }

        // Restart: recovery loads all segments; appends continue at offset 5.
        try (Log log = new Log(file, segBytes)) {
            assertEquals(5, log.endOffset(), "recovered LEO");
            assertEquals(5, log.read(0, 1 << 20).size(), "all records survived restart");
            assertEquals(5, log.append(rec(5)), "next append continues the offset sequence");
            assertEquals(6, log.endOffset());
        }
    }
}
