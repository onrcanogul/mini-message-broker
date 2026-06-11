package minibroker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * A single .log file, named by its base offset (the first offset it can hold),
 * e.g. baseOffset 0 -> "00000000000000000000.log". This is the file-level unit
 * a Log is composed of (one segment for now; multiple segments in Part 2).
 *
 * On-disk record format is unchanged from v0 (big-endian, sequential):
 *   recLen(int32) | offset(int64) | timestamp(int64) |
 *   keyLen(int32, -1=null) | key | valLen(int32) | value | crc32(int32)
 *
 * The in-memory position index is segment-local: positions.get(offset - baseOffset)
 * -> byte position. (A persistent/sparse index is Part 3.)
 *
 * All methods are synchronized: serialized read/write under a single lock.
 */
public class LogSegment implements AutoCloseable {

    /** Kafka-style 20-digit zero-padded segment file name. */
    public static String fileName(long baseOffset) {
        return String.format("%020d.log", baseOffset);
    }

    private final long baseOffset;
    private final Path file;
    private final FileChannel channel;
    private final List<Long> positions = new ArrayList<>(); // index = offset - baseOffset
    private long nextOffset;     // absolute LEO of this segment
    private long writePosition;  // end of file (new records are written here)

    public LogSegment(Path dir, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;
        this.file = dir.resolve(fileName(baseOffset));
        this.channel = FileChannel.open(file,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.nextOffset = baseOffset;
        recover();
    }

    /**
     * Crash recovery for this segment: scan from the start, keep every intact record,
     * truncate the file at the first partial/corrupt record. Identical policy to v0,
     * but offsets are counted from baseOffset.
     */
    private void recover() throws IOException {
        long fileSize = channel.size();
        long pos = 0;

        while (pos < fileSize) {
            if (fileSize - pos < 4) break; // partial recLen at the tail
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, pos);
            lenBuf.flip();
            int recLen = lenBuf.getInt();

            // Garbage recLen or a record that runs past EOF = corrupt/partial tail.
            if (recLen <= 0 || fileSize - (pos + 4) < recLen) break;

            ByteBuffer rec = ByteBuffer.allocate(recLen);
            channel.read(rec, pos + 4);
            rec.flip();
            int bodyLen = recLen - 4; // everything except the trailing crc
            CRC32 crc = new CRC32();
            crc.update(rec.array(), 0, bodyLen);
            int storedCrc = rec.getInt(bodyLen);
            if ((int) crc.getValue() != storedCrc) break; // corrupt -> tail starts here

            positions.add(pos);
            nextOffset++;
            pos += 4 + recLen;
        }

        if (pos < fileSize) {
            channel.truncate(pos); // drop the partial/corrupt tail
        }
        this.writePosition = pos;
    }

    /** Appends to the end, assigns the absolute offset and returns it. */
    public synchronized long append(Record record) throws IOException {
        long offset = nextOffset;
        byte[] key = record.key();
        byte[] value = record.value();
        int keyLen = (key == null) ? -1 : key.length;
        int valLen = value.length;

        int bodyLen = 8 + 8 + 4 + (keyLen < 0 ? 0 : keyLen) + 4 + valLen; // offset..value
        int recLen = bodyLen + 4;

        ByteBuffer buf = ByteBuffer.allocate(4 + recLen); // ByteBuffer defaults to big-endian
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
        channel.write(buf, writePosition);

        positions.add(writePosition);
        writePosition += buf.limit();
        nextOffset++;
        return offset;
    }

    /**
     * Reads sequentially from fetchOffset, but ONLY within this segment's bounds.
     * Returns at least one record when fetchOffset is in range (progress guarantee).
     */
    public synchronized List<StoredRecord> read(long fetchOffset, int maxBytes) throws IOException {
        List<StoredRecord> out = new ArrayList<>();
        if (fetchOffset < baseOffset || fetchOffset >= nextOffset) return out;

        long pos = positions.get((int) (fetchOffset - baseOffset));
        int consumed = 0;
        while (pos < writePosition) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (channel.read(lenBuf, pos) < 4) break;
            lenBuf.flip();
            int recLen = lenBuf.getInt();
            int totalLen = 4 + recLen;

            if (!out.isEmpty() && consumed + totalLen > maxBytes) break;

            ByteBuffer rec = ByteBuffer.allocate(recLen);
            channel.read(rec, pos + 4);
            rec.flip();
            long offset = rec.getLong();
            long timestamp = rec.getLong();
            int keyLen = rec.getInt();
            byte[] key = null;
            if (keyLen >= 0) {
                key = new byte[keyLen];
                rec.get(key);
            }
            int valLen = rec.getInt();
            byte[] value = new byte[valLen];
            rec.get(value);

            out.add(new StoredRecord(offset, timestamp, key, value));
            consumed += totalLen;
            pos += totalLen;
        }
        return out;
    }

    /** First offset this segment holds (also encoded in its file name). */
    public long baseOffset() {
        return baseOffset;
    }

    /** Next offset that will be written = LEO of this segment. */
    public synchronized long endOffset() {
        return nextOffset;
    }

    /** Bytes currently written to the segment file. */
    public synchronized long sizeInBytes() {
        return writePosition;
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
