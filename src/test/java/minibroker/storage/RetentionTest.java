package minibroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class RetentionTest {

    private static Record rec(String v, long ts) {
        return new Record(v.getBytes(UTF_8), v.getBytes(UTF_8), ts);
    }

    private static long recordDiskSize() {
        // value "vNN" = 3 bytes, key same: recLen(4) + body(8+8+4+3+4+3) + crc(4)
        return 4 + (8 + 8 + 4 + 3 + 4 + 3) + 4;
    }

    private static boolean segmentExists(Path dir, long base) {
        return Files.exists(dir.resolve(LogSegment.fileName(base)))
                || Files.exists(dir.resolve(LogSegment.indexFileName(base)));
    }

    @Test
    void timeBasedDeletesOldClosedSegments(@TempDir Path dir) throws Exception {
        long segBytes = 2 * recordDiskSize(); // ~2 records per segment
        AtomicLong now = new AtomicLong(10_000);
        long retentionMs = 1_000; // keep ~ last 1000 ms

        try (Log log = new Log(dir.resolve("partition.log"), segBytes,
                LogSegment.DEFAULT_INDEX_INTERVAL, retentionMs, Log.RETENTION_OFF, now::get)) {
            // offsets 0..3 are OLD; 4,5 are RECENT.
            log.append(rec("v00", 100));   // 0  seg0
            log.append(rec("v01", 100));   // 1  seg0 -> roll
            log.append(rec("v02", 100));   // 2  seg2
            log.append(rec("v03", 100));   // 3  seg2 -> roll
            log.append(rec("v04", 9_900)); // 4  seg4
            log.append(rec("v05", 9_900)); // 5  seg4 -> roll -> seg6 (active, empty)
            assertEquals(6, log.endOffset());
            assertEquals(0, log.logStartOffset());

            log.enforceRetention(); // now=10000, cutoff=9000 -> seg0,seg2 old; seg4 recent

            assertFalse(segmentExists(dir, 0), "seg0 files deleted");
            assertFalse(segmentExists(dir, 2), "seg2 files deleted");
            assertTrue(segmentExists(dir, 4), "seg4 (recent) kept");
            assertEquals(4, log.logStartOffset(), "log start advanced past deleted data");
            assertTrue(log.read(0, 1 << 20).isEmpty(), "deleted offset returns nothing");
            assertEquals(4, log.read(4, 1 << 20).get(0).offset(), "recent data still readable");
        }
    }

    @Test
    void sizeBasedDeletesUntilUnderCap(@TempDir Path dir) throws Exception {
        long segBytes = 2 * recordDiskSize();          // ~2 records per segment
        long retentionBytes = 3 * recordDiskSize();    // keep ~1.5 segments' worth

        try (Log log = new Log(dir.resolve("partition.log"), segBytes,
                LogSegment.DEFAULT_INDEX_INTERVAL, Log.RETENTION_OFF, retentionBytes, () -> 0L)) {
            for (int i = 0; i < 10; i++) log.append(rec(String.format("v%02d", i), 0));
            assertEquals(10, log.endOffset());

            log.enforceRetention();

            // Total kept must be under the cap (active segment excluded from deletion).
            long total = 0;
            for (long base = log.logStartOffset(); base < 10; ) {
                if (segmentExists(dir, base)) total += Files.size(dir.resolve(LogSegment.fileName(base)));
                base += 2;
            }
            assertTrue(total <= retentionBytes, "total closed-segment size under the cap");
            assertTrue(log.logStartOffset() > 0, "oldest data deleted");
            assertFalse(segmentExists(dir, 0), "oldest segment gone");
            assertTrue(log.read(log.logStartOffset(), 1 << 20).size() > 0, "remaining data readable");
        }
    }

    @Test
    void restartLoadsOnlyRemainingSegments(@TempDir Path dir) throws Exception {
        long segBytes = 2 * recordDiskSize();
        AtomicLong now = new AtomicLong(10_000);

        try (Log log = new Log(dir.resolve("partition.log"), segBytes,
                LogSegment.DEFAULT_INDEX_INTERVAL, 1_000, Log.RETENTION_OFF, now::get)) {
            for (int i = 0; i < 4; i++) log.append(rec(String.format("v%02d", i), 100)); // old
            log.append(rec("v04", 9_900));
            log.append(rec("v05", 9_900));
            log.enforceRetention();
            assertEquals(4, log.logStartOffset());
        }

        // Reopen: only the surviving segments are on disk, so recovery is consistent.
        try (Log log = new Log(dir.resolve("partition.log"), segBytes,
                LogSegment.DEFAULT_INDEX_INTERVAL, 1_000, Log.RETENTION_OFF, now::get)) {
            assertEquals(4, log.logStartOffset(), "deleted segments stay gone after restart");
            assertEquals(6, log.endOffset());
            assertEquals(2, log.read(4, 1 << 20).size(), "remaining records readable");
            assertTrue(log.read(0, 1 << 20).isEmpty(), "deleted offset still gone");
        }
    }
}
