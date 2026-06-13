package minibroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * A single .log file (named by base offset) plus a persistent, SPARSE .index file
 * that replaces the old dense in-memory offset->position map.
 *
 * .log record format is unchanged (big-endian):
 *   recLen(int32) | offset(int64) | timestamp(int64) |
 *   keyLen(int32, -1=null) | key | valLen(int32) | value | crc32(int32)
 *
 * .index entry format (fixed 8 bytes, so it is binary-searchable):
 *   relativeOffset(int32 = absoluteOffset - baseOffset) | position(int32 = byte pos in .log)
 *
 * One index entry is written per ~indexInterval log bytes. To find offset O:
 * binary-search the index for the largest entry with relativeOffset <= (O - base),
 * jump to that .log position, then scan forward (at most one interval) to reach O.
 */
public class LogSegment implements AutoCloseable {

    /** Default sparse-index interval if none is given. */
    public static final int DEFAULT_INDEX_INTERVAL = 4096;

    private static final int INDEX_ENTRY_BYTES = 8;

    public static String fileName(long baseOffset) {
        return String.format("%020d.log", baseOffset);
    }

    public static String indexFileName(long baseOffset) {
        return String.format("%020d.index", baseOffset);
    }

    private enum Mode {ACTIVE, CLOSED}

    private final long baseOffset;
    private final int indexInterval;
    private final Path logFile;
    private final Path indexFile;
    private final FileChannel logChannel;
    private final FileChannel indexChannel;
    private long lastTimestamp = Long.MIN_VALUE; // newest record's timestamp (for time retention)

    // In-memory mirror of the sparse index, ascending by relativeOffset.
    private final List<int[]> index = new ArrayList<>(); // each: {relativeOffset, position}
    private long indexWritePosition;

    private long nextOffset;     // absolute LEO of this segment
    private long writePosition;  // end of .log (new records are written here)
    private int bytesSinceLastIndexEntry;

    /** Active/fresh segment (default interval). */
    public LogSegment(Path dir, long baseOffset) throws IOException {
        this(dir, baseOffset, DEFAULT_INDEX_INTERVAL);
    }

    /** Active/fresh segment: scans the .log, truncates a corrupt tail, rebuilds the index. */
    public LogSegment(Path dir, long baseOffset, int indexInterval) throws IOException {
        this(dir, baseOffset, indexInterval, Mode.ACTIVE, -1);
    }

    /**
     * Closed (sealed) segment: trust the on-disk index, do NOT scan the .log.
     * endOffset is supplied by the caller (it equals the next segment's base offset).
     */
    public static LogSegment openClosed(Path dir, long baseOffset, long endOffset, int indexInterval)
            throws IOException {
        return new LogSegment(dir, baseOffset, indexInterval, Mode.CLOSED, endOffset);
    }

    private LogSegment(Path dir, long baseOffset, int indexInterval, Mode mode, long knownEndOffset)
            throws IOException {
        this.baseOffset = baseOffset;
        this.indexInterval = indexInterval;
        this.nextOffset = baseOffset;
        this.logFile = dir.resolve(fileName(baseOffset));
        this.indexFile = dir.resolve(indexFileName(baseOffset));
        this.logChannel = FileChannel.open(logFile,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.indexChannel = FileChannel.open(indexFile,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        if (mode == Mode.ACTIVE) {
            recoverAndRebuildIndex(); // scan .log, truncate tail, rewrite .index
        } else {
            loadIndex();                              // trust the persisted index
            this.nextOffset = knownEndOffset;
            this.writePosition = logChannel.size();
            // newest timestamp via the last record only (bounded read, no full scan)
            this.lastTimestamp = (nextOffset > baseOffset) ? readLastTimestamp() : Long.MIN_VALUE;
        }
    }

    private long readLastTimestamp() throws IOException {
        List<StoredRecord> last = read(nextOffset - 1, 1);
        return last.isEmpty() ? Long.MIN_VALUE : last.get(0).timestamp();
    }

    // ===================== recovery =====================

    /** Reads the persisted .index file into memory (closed segments). */
    private void loadIndex() throws IOException {
        long size = indexChannel.size();
        ByteBuffer buf = ByteBuffer.allocate((int) size);
        indexChannel.read(buf, 0);
        buf.flip();
        while (buf.remaining() >= INDEX_ENTRY_BYTES) {
            index.add(new int[]{buf.getInt(), buf.getInt()});
        }
        this.indexWritePosition = (long) index.size() * INDEX_ENTRY_BYTES;
    }

    /**
     * Active-segment recovery: scan .log keeping intact records, truncate at the first
     * partial/corrupt one, and rebuild the sparse index from scratch (so it can never
     * point past the truncated, valid end of the log).
     */
    private void recoverAndRebuildIndex() throws IOException {
        long fileSize = logChannel.size();
        long pos = 0;
        int sinceLast = 0;

        while (pos < fileSize) {
            if (fileSize - pos < 4) break;
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            logChannel.read(lenBuf, pos);
            lenBuf.flip();
            int recLen = lenBuf.getInt();
            if (recLen <= 0 || fileSize - (pos + 4) < recLen) break;

            ByteBuffer rec = ByteBuffer.allocate(recLen);
            logChannel.read(rec, pos + 4);
            rec.flip();
            int bodyLen = recLen - 4;
            CRC32 crc = new CRC32();
            crc.update(rec.array(), 0, bodyLen);
            if ((int) crc.getValue() != rec.getInt(bodyLen)) break; // corrupt -> tail

            long offset = rec.getLong(0); // first body field
            lastTimestamp = rec.getLong(8); // second body field (timestamp)
            int totalLen = 4 + recLen;
            sinceLast += totalLen;
            if (sinceLast >= indexInterval) {
                index.add(new int[]{(int) (offset - baseOffset), (int) pos});
                sinceLast = 0;
            }
            nextOffset++;
            pos += totalLen;
        }

        if (pos < fileSize) {
            logChannel.truncate(pos); // drop partial/corrupt tail
        }
        this.writePosition = pos;
        this.bytesSinceLastIndexEntry = sinceLast;

        // Persist the freshly rebuilt index (replacing any stale on-disk one).
        indexChannel.truncate(0);
        this.indexWritePosition = 0;
        for (int[] e : index) {
            writeIndexEntryToFile(e[0], e[1]);
        }
    }

    // ===================== append =====================

    public synchronized long append(Record record) throws IOException {
        long offset = nextOffset;
        byte[] key = record.key();
        byte[] value = record.value();
        int keyLen = (key == null) ? -1 : key.length;
        int valLen = value.length;

        int bodyLen = 8 + 8 + 4 + (keyLen < 0 ? 0 : keyLen) + 4 + valLen;
        int recLen = bodyLen + 4;

        ByteBuffer buf = ByteBuffer.allocate(4 + recLen);
        buf.putInt(recLen);
        int crcStart = buf.position();
        buf.putLong(offset);
        buf.putLong(record.timestamp());
        buf.putInt(keyLen);
        if (keyLen >= 0) buf.put(key);
        buf.putInt(valLen);
        buf.put(value);

        CRC32 crc = new CRC32();
        crc.update(buf.array(), crcStart, buf.position() - crcStart);
        buf.putInt((int) crc.getValue());

        buf.flip();
        long recordPos = writePosition;
        logChannel.write(buf, recordPos);

        writePosition += buf.limit();
        nextOffset++;
        lastTimestamp = record.timestamp();

        // Sparse index: one entry per ~indexInterval bytes.
        bytesSinceLastIndexEntry += buf.limit();
        if (bytesSinceLastIndexEntry >= indexInterval) {
            writeIndexEntryToFile((int) (offset - baseOffset), (int) recordPos);
            index.add(new int[]{(int) (offset - baseOffset), (int) recordPos});
            bytesSinceLastIndexEntry = 0;
        }
        return offset;
    }

    private void writeIndexEntryToFile(int relativeOffset, int position) throws IOException {
        ByteBuffer e = ByteBuffer.allocate(INDEX_ENTRY_BYTES);
        e.putInt(relativeOffset);
        e.putInt(position);
        e.flip();
        indexChannel.write(e, indexWritePosition);
        indexWritePosition += INDEX_ENTRY_BYTES;
    }

    // ===================== read =====================

    public synchronized List<StoredRecord> read(long fetchOffset, int maxBytes) throws IOException {
        List<StoredRecord> out = new ArrayList<>();
        if (fetchOffset < baseOffset || fetchOffset >= nextOffset) return out;

        long pos = floorPosition(fetchOffset); // jump near the target via the index
        int consumed = 0;
        while (pos < writePosition) {
            ByteBuffer header = ByteBuffer.allocate(12); // recLen(4) + offset(8)
            if (logChannel.read(header, pos) < 12) break;
            header.flip();
            int recLen = header.getInt();
            long offset = header.getLong();
            int totalLen = 4 + recLen;

            if (offset < fetchOffset) { // still scanning forward to the target
                pos += totalLen;
                continue;
            }
            if (!out.isEmpty() && consumed + totalLen > maxBytes) break;

            ByteBuffer rec = ByteBuffer.allocate(recLen);
            logChannel.read(rec, pos + 4);
            rec.flip();
            long o = rec.getLong();
            long ts = rec.getLong();
            int keyLen = rec.getInt();
            byte[] key = null;
            if (keyLen >= 0) {
                key = new byte[keyLen];
                rec.get(key);
            }
            int valLen = rec.getInt();
            byte[] value = new byte[valLen];
            rec.get(value);

            out.add(new StoredRecord(o, ts, key, value));
            consumed += totalLen;
            pos += totalLen;
        }
        return out;
    }

    /**
     * Byte position in .log to START scanning from for the given offset: the position
     * of the largest indexed entry with relativeOffset <= (offset - baseOffset), or 0
     * if no such entry (scan from the beginning). Binary search over the fixed-size index.
     */
    public synchronized int floorPosition(long offset) {
        int rel = (int) (offset - baseOffset);
        int lo = 0, hi = index.size() - 1, ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (index.get(mid)[0] <= rel) {
                ans = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return ans < 0 ? 0 : index.get(ans)[1];
    }

    // ===================== accessors =====================

    public long baseOffset() {
        return baseOffset;
    }

    public synchronized long endOffset() {
        return nextOffset;
    }

    public synchronized long sizeInBytes() {
        return writePosition;
    }

    /** Number of sparse index entries (for tests/observability). */
    public synchronized int indexEntryCount() {
        return index.size();
    }

    /** Timestamp of this segment's newest record (Long.MIN_VALUE if empty). */
    public synchronized long lastTimestamp() {
        return lastTimestamp;
    }

    /** Closes channels and removes both the .log and .index files from disk. */
    public synchronized void delete() throws IOException {
        close();
        Files.deleteIfExists(logFile);
        Files.deleteIfExists(indexFile);
    }

    @Override
    public synchronized void close() throws IOException {
        logChannel.close();
        indexChannel.close();
    }
}
