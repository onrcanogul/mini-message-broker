package minibroker;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Append-only log file for a single partition.
 *
 * On-disk format (big-endian, records laid out sequentially):
 *   recLen(int32) | offset(int64) | timestamp(int64) |
 *   keyLen(int32, -1=null) | key | valLen(int32) | value | crc32(int32)
 *
 * recLen = number of bytes from offset..crc (used to skip records and to
 * detect a half-written tail; we'll use the latter for recovery in Part 2).
 *
 * All methods are synchronized: serialized read/write under a single lock.
 */
public class Log implements AutoCloseable {

    private final FileChannel channel;
    // In-memory offset index: positions.get(offset) -> byte position in the file.
    // Offsets are contiguous from 0, so an ArrayList is enough.
    private final List<Long> positions = new ArrayList<>();
    private long nextOffset = 0;    // next offset to assign = LEO
    private long writePosition = 0; // end of file (new records are written here)

    public Log(Path file) throws IOException {
        this.channel = FileChannel.open(file,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        recover();
    }

    /**
     * Crash recovery: scan the existing file from the start, treating the on-disk
     * bytes as the single source of truth, and rebuild the in-memory state
     * (positions index, nextOffset, writePosition).
     *
     * For each record: check the whole record is present (remaining >= recLen) and
     * the CRC matches. The first partial or corrupt record marks the tail: we
     * truncate the file at its start and stop. Everything before it is durable.
     */
    private void recover() throws IOException {
        long fileSize = channel.size();
        long pos = 0;

        while (pos < fileSize) {
            // 1) Need 4 bytes for recLen; otherwise the tail is a partial record.
            if (fileSize - pos < 4) break;
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            channel.read(lenBuf, pos);
            lenBuf.flip();
            int recLen = lenBuf.getInt();

            // 2) Sanity + "is the whole record on disk?" check.
            //    A garbage recLen (<=0) or a record that runs past EOF = corrupt/partial tail.
            if (recLen <= 0 || fileSize - (pos + 4) < recLen) break;

            // 3) Read the full record (offset..crc) and verify CRC over offset..value.
            ByteBuffer rec = ByteBuffer.allocate(recLen);
            channel.read(rec, pos + 4);
            rec.flip();
            int bodyLen = recLen - 4; // everything except the trailing crc
            CRC32 crc = new CRC32();
            crc.update(rec.array(), 0, bodyLen);
            int storedCrc = rec.getInt(bodyLen); // last 4 bytes
            if ((int) crc.getValue() != storedCrc) break; // corrupt -> tail starts here

            // 4) Intact record: record its position and advance.
            positions.add(pos);
            nextOffset++;
            pos += 4 + recLen;
        }

        // Truncate any partial/corrupt tail and set the write cursor to the clean end.
        if (pos < fileSize) {
            channel.truncate(pos);
        }
        this.writePosition = pos;
    }

    /** Appends to the end, assigns the offset and returns it. */
    public synchronized long append(Record record) throws IOException {
        long offset = nextOffset;
        byte[] key = record.key();
        byte[] value = record.value();
        int keyLen = (key == null) ? -1 : key.length;
        int valLen = value.length;

        // recLen = offset..value (body) + crc(4). If keyLen=-1 there are no key bytes.
        int bodyLen = 8 + 8 + 4 + (keyLen < 0 ? 0 : keyLen) + 4 + valLen;
        int recLen = bodyLen + 4;

        ByteBuffer buf = ByteBuffer.allocate(4 + recLen); // ByteBuffer defaults to big-endian
        buf.putInt(recLen);
        int crcStart = buf.position();          // crc is computed over offset..value
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
        channel.write(buf, writePosition);      // positional write; does not move the channel cursor

        positions.add(writePosition);
        writePosition += buf.limit();
        nextOffset++;
        return offset;
    }

    /**
     * Scans sequentially starting at fetchOffset; returns records up to ~maxBytes total.
     * Always returns at least one record (progress guarantee for a single large record).
     */
    public synchronized List<StoredRecord> read(long fetchOffset, int maxBytes) throws IOException {
        List<StoredRecord> out = new ArrayList<>();
        if (fetchOffset < 0 || fetchOffset >= nextOffset) return out;

        long pos = positions.get((int) fetchOffset);
        int consumed = 0;
        while (pos < writePosition) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (channel.read(lenBuf, pos) < 4) break;
            lenBuf.flip();
            int recLen = lenBuf.getInt();
            int totalLen = 4 + recLen; // whole record including the recLen field

            // maxBytes limit: always take the first record, stop later ones if the limit is exceeded.
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
            // crc validation will come in Part 2 (recovery / corrupt-tail detection).

            out.add(new StoredRecord(offset, timestamp, key, value));
            consumed += totalLen;
            pos += totalLen;
        }
        return out;
    }

    /** Log End Offset: the next offset that will be written. */
    public synchronized long endOffset() {
        return nextOffset;
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}
