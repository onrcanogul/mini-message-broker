package minibroker.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A partition's log: an ordered sequence of LogSegments (by base offset), exactly
 * one of which is "active" (the last). Offsets increase monotonically ACROSS
 * segments — a segment boundary never resets them.
 *
 * Public API (append / read / endOffset) is unchanged from v0.
 *
 * The constructor takes a path for backward compatibility; only its PARENT
 * directory matters — that's the partition directory the segment files live in.
 */
public class Log implements AutoCloseable {

    private final Path dir;
    private final long segmentBytes;                         // roll threshold
    private final List<LogSegment> segments = new ArrayList<>(); // sorted by baseOffset asc
    private LogSegment active;                                // == last segment

    /** Single-segment Log (never rolls). Keeps the old one-arg contract. */
    public Log(Path partitionFile) throws IOException {
        this(partitionFile, Long.MAX_VALUE);
    }

    public Log(Path partitionFile, long segmentBytes) throws IOException {
        this.dir = partitionFile.toAbsolutePath().getParent();
        this.segmentBytes = segmentBytes;
        load();
    }

    /**
     * Startup recovery: discover every .log file, load them in base-offset order.
     * Each LogSegment recovers itself, but in practice only the ACTIVE (last)
     * segment can have a partial tail from a crash — older segments were sealed
     * before rolling and are treated as complete.
     */
    private void load() throws IOException {
        List<Long> bases = new ArrayList<>();
        if (Files.isDirectory(dir)) {
            try (var entries = Files.list(dir)) {
                entries.map(p -> p.getFileName().toString())
                        .filter(n -> n.endsWith(".log"))
                        .forEach(n -> bases.add(Long.parseLong(n.substring(0, n.length() - 4))));
            }
        }
        bases.sort(Long::compareTo);

        if (bases.isEmpty()) {
            active = new LogSegment(dir, 0L); // fresh partition
            segments.add(active);
            return;
        }
        for (long base : bases) {
            segments.add(new LogSegment(dir, base));
        }
        active = segments.get(segments.size() - 1);
    }

    /** Appends to the active segment, rolling to a new one if it grew past segmentBytes. */
    public synchronized long append(Record record) throws IOException {
        long offset = active.append(record);
        if (active.sizeInBytes() >= segmentBytes) {
            roll();
        }
        return offset;
    }

    private void roll() throws IOException {
        // New segment starts exactly where the active one ended -> contiguous offsets.
        LogSegment next = new LogSegment(dir, active.endOffset());
        segments.add(next);
        active = next; // the old active stays open (immutable) for reads
    }

    /**
     * Reads from fetchOffset, crossing segment boundaries as needed until ~maxBytes
     * is reached or there are no more records. A single call can return records from
     * several segments.
     */
    public synchronized List<StoredRecord> read(long fetchOffset, int maxBytes) throws IOException {
        List<StoredRecord> out = new ArrayList<>();
        int idx = segmentIndexFor(fetchOffset);
        if (idx < 0) return out;

        long start = fetchOffset;
        int remaining = maxBytes;
        for (int i = idx; i < segments.size(); i++) {
            LogSegment seg = segments.get(i);
            List<StoredRecord> part = seg.read(start, remaining);
            if (part.isEmpty()) break;
            out.addAll(part);
            for (StoredRecord r : part) remaining -= diskSize(r);

            long lastOffset = part.get(part.size() - 1).offset();
            // If we stopped before this segment's end, maxBytes cut us off -> done.
            if (lastOffset + 1 < seg.endOffset()) break;
            if (remaining <= 0) break;
            start = seg.endOffset(); // continue at the next segment's base offset
        }
        return out;
    }

    /** Index of the segment owning fetchOffset: the largest baseOffset <= fetchOffset. */
    private int segmentIndexFor(long fetchOffset) {
        if (fetchOffset < 0 || fetchOffset >= active.endOffset()) return -1;
        int found = -1;
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i).baseOffset() <= fetchOffset) found = i;
            else break;
        }
        return found;
    }

    /** Log End Offset: the next offset that will be written (the active segment's end). */
    public synchronized long endOffset() {
        return active.endOffset();
    }

    @Override
    public synchronized void close() throws IOException {
        for (LogSegment s : segments) s.close();
    }

    /** On-disk byte size of a stored record (recLen + body + crc); mirrors LogSegment's layout. */
    private static int diskSize(StoredRecord r) {
        int keyLen = (r.key() == null) ? 0 : r.key().length;
        int body = 8 + 8 + 4 + keyLen + 4 + r.value().length;
        return 4 + body + 4;
    }
}
