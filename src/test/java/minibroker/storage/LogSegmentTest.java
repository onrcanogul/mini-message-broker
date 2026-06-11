package minibroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class LogSegmentTest {

    @Test
    void appendReadAndZeroPaddedFileName(@TempDir Path dir) throws Exception {
        try (LogSegment seg = new LogSegment(dir, 0L)) {
            assertEquals(0, seg.append(new Record("k0".getBytes(UTF_8), "v0".getBytes(UTF_8), 1000)));
            assertEquals(1, seg.append(new Record(null,                "v1".getBytes(UTF_8), 1001)));
            assertEquals(2, seg.append(new Record("k2".getBytes(UTF_8), "v2".getBytes(UTF_8), 1002)));
            assertEquals(0, seg.baseOffset());
            assertEquals(3, seg.endOffset());

            List<StoredRecord> recs = seg.read(0, 1 << 20);
            assertEquals(3, recs.size());
            assertEquals(0, recs.get(0).offset());
            assertArrayEquals("v0".getBytes(UTF_8), recs.get(0).value());
            assertNull(recs.get(1).key());
            assertEquals(2, recs.get(2).offset());
            assertArrayEquals("v2".getBytes(UTF_8), recs.get(2).value());
        }

        // File name = base offset, zero-padded to 20 digits.
        Path expected = dir.resolve("00000000000000000000.log");
        assertEquals("00000000000000000000.log", LogSegment.fileName(0));
        assertTrue(Files.exists(expected), "segment file should be the zero-padded base offset");
    }

    @Test
    void reopenRecoversRecords(@TempDir Path dir) throws Exception {
        try (LogSegment seg = new LogSegment(dir, 0L)) {
            seg.append(new Record("k0".getBytes(UTF_8), "v0".getBytes(UTF_8), 1000));
            seg.append(new Record("k1".getBytes(UTF_8), "v1".getBytes(UTF_8), 1001));
            seg.append(new Record("k2".getBytes(UTF_8), "v2".getBytes(UTF_8), 1002));
        }

        // Reopen the same file: state must be rebuilt purely from disk.
        try (LogSegment seg = new LogSegment(dir, 0L)) {
            assertEquals(3, seg.endOffset());
            List<StoredRecord> recs = seg.read(0, 1 << 20);
            assertEquals(3, recs.size());
            assertArrayEquals("v0".getBytes(UTF_8), recs.get(0).value());
            assertArrayEquals("v2".getBytes(UTF_8), recs.get(2).value());
        }
    }

    @Test
    void nonZeroBaseOffsetNamingAndOffsets(@TempDir Path dir) throws Exception {
        // A segment that starts at offset 50 names its file accordingly and assigns
        // absolute offsets from its base — the groundwork for multiple segments (Part 2).
        try (LogSegment seg = new LogSegment(dir, 50L)) {
            assertEquals(50, seg.append(new Record(null, "a".getBytes(UTF_8), 0)));
            assertEquals(51, seg.append(new Record(null, "b".getBytes(UTF_8), 0)));
            assertEquals(52, seg.endOffset());

            List<StoredRecord> recs = seg.read(51, 1 << 20);
            assertEquals(1, recs.size());
            assertEquals(51, recs.get(0).offset());
            assertArrayEquals("b".getBytes(UTF_8), recs.get(0).value());

            assertTrue(seg.read(0, 1 << 20).isEmpty(), "offset below baseOffset is out of this segment");
        }
        assertTrue(Files.exists(dir.resolve("00000000000000000050.log")));
    }
}
