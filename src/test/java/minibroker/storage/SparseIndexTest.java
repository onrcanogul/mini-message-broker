package minibroker.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class SparseIndexTest {

    // Fixed-width records so the index interval lands predictably.
    private static Record rec(int i) {
        return new Record(String.format("k%02d", i).getBytes(UTF_8),
                String.format("v%02d", i).getBytes(UTF_8), 1000 + i);
    }

    private static long recordDiskSize() {
        // recLen(4) + body(offset8 + ts8 + keyLen4 + key3 + valLen4 + value3) + crc(4)
        return 4 + (8 + 8 + 4 + 3 + 4 + 3) + 4;
    }

    /** Small interval so a single segment gets several index entries. */
    private static int smallInterval() {
        return (int) (2 * recordDiskSize());
    }

    @Test
    void buildsMultipleEntriesAndLooksUpViaIndex(@TempDir Path dir) throws Exception {
        int interval = smallInterval();
        try (LogSegment seg = new LogSegment(dir, 0L, interval)) {
            for (int i = 0; i < 15; i++) seg.append(rec(i));

            // The .index file exists and holds more than one entry.
            assertTrue(Files.exists(dir.resolve("00000000000000000000.index")));
            assertTrue(seg.indexEntryCount() > 1, "expected several sparse index entries");

            // Lookup for offset 7 returns the right record...
            List<StoredRecord> got = seg.read(7, 1 << 20);
            assertEquals(7, got.get(0).offset());
            assertArrayEquals("v07".getBytes(UTF_8), got.get(0).value());

            // ...and the scan started from an index mark, not the beginning of the file.
            assertTrue(seg.floorPosition(7) > 0, "search should jump via the index, not scan from 0");
        }
    }

    @Test
    void closedSegmentLoadsIndexFromDisk(@TempDir Path dir) throws Exception {
        int interval = smallInterval();
        long endOffset;
        try (LogSegment seg = new LogSegment(dir, 0L, interval)) {
            for (int i = 0; i < 15; i++) seg.append(rec(i));
            endOffset = seg.endOffset();
        }

        // Reopen as CLOSED: this does NOT scan the .log — the index must come from disk.
        try (LogSegment seg = LogSegment.openClosed(dir, 0L, endOffset, interval)) {
            assertTrue(seg.indexEntryCount() > 1, "index entries loaded from the .index file");
            assertTrue(seg.floorPosition(7) > 0, "disk-loaded index still drives the jump");
            List<StoredRecord> got = seg.read(7, 1 << 20);
            assertEquals(7, got.get(0).offset());
            assertArrayEquals("v07".getBytes(UTF_8), got.get(0).value());
        }
    }

    @Test
    void activeRecoveryRebuildsConsistentIndexAfterCorruptTail(@TempDir Path dir) throws Exception {
        int interval = smallInterval();
        try (LogSegment seg = new LogSegment(dir, 0L, interval)) {
            for (int i = 0; i < 15; i++) seg.append(rec(i));
        }

        // Append a half-written record (bogus large recLen) to the .log tail.
        Path logFile = dir.resolve("00000000000000000000.log");
        try (FileChannel ch = FileChannel.open(logFile, StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(new byte[]{0, 0, 0, 99, 1, 2, 3, 4}), ch.size());
        }

        // Reopen as ACTIVE: recovery truncates the tail and rebuilds the index from
        // the valid records, so the index never points past the valid log end.
        try (LogSegment seg = new LogSegment(dir, 0L, interval)) {
            assertEquals(15, seg.endOffset(), "corrupt tail dropped, 15 records intact");

            // Every offset is still readable -> index entries are all within valid data.
            assertEquals(15, seg.read(0, 1 << 20).size());
            List<StoredRecord> mid = seg.read(7, 1 << 20);
            assertEquals(7, mid.get(0).offset());
            assertArrayEquals("v07".getBytes(UTF_8), mid.get(0).value());
            assertEquals(14, seg.read(14, 1 << 20).get(0).offset());
            assertTrue(seg.floorPosition(7) > 0);
        }
    }
}
