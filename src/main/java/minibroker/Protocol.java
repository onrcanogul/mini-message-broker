package minibroker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Serialization only — no sockets. All multi-byte integers are big-endian
 * (DataInput/DataOutputStream guarantee this), so Part 4 can plug a socket's
 * streams straight into readFrame/writeFrame.
 *
 * encode* methods return the PAYLOAD (header + body), without the frame length.
 * writeFrame prepends the length when the payload goes onto the wire.
 */
public final class Protocol {

    private Protocol() {
    }

    /** A decoded request: apiKey + correlationId from the header, plus the typed body. */
    public record DecodedRequest(ApiKey apiKey, int correlationId, Object body) {
    }

    /** A decoded response: correlationId from the header, plus the typed body. */
    public record DecodedResponse(int correlationId, Object body) {
    }

    // ===================== Frame primitives (stream-based) =====================

    /** frameLen(int32) + payload. */
    public static void writeFrame(OutputStream raw, byte[] payload) throws IOException {
        DataOutputStream out = new DataOutputStream(raw);
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    /** Read the 4-byte length, then exactly that many bytes; return the payload. */
    public static byte[] readFrame(InputStream raw) throws IOException {
        DataInputStream in = new DataInputStream(raw);
        int frameLen = in.readInt();   // blocks until 4 bytes are available
        byte[] payload = new byte[frameLen];
        in.readFully(payload);         // blocks until the whole frame arrives
        return payload;
    }

    // ===================== Requests =====================

    public static byte[] encodeRequest(int correlationId, ProduceRequest req) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(buf);
        d.writeShort(ApiKey.PRODUCE.id());   // header: apiKey
        d.writeInt(correlationId);           // header: correlationId
        writeString(d, req.topic());
        d.writeInt(req.partition());
        d.writeInt(req.records().size());
        for (Record r : req.records()) {
            writeBytes(d, r.key());
            writeBytes(d, r.value());
        }
        return buf.toByteArray();
    }

    public static byte[] encodeRequest(int correlationId, FetchRequest req) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(buf);
        d.writeShort(ApiKey.FETCH.id());
        d.writeInt(correlationId);
        writeString(d, req.topic());
        d.writeInt(req.partition());
        d.writeLong(req.fetchOffset());
        d.writeInt(req.maxBytes());
        return buf.toByteArray();
    }

    /** Dispatch by apiKey (present in the request header). */
    public static DecodedRequest decodeRequest(byte[] payload) throws IOException {
        DataInputStream d = new DataInputStream(new ByteArrayInputStream(payload));
        ApiKey apiKey = ApiKey.fromId(d.readShort());
        int correlationId = d.readInt();
        Object body = switch (apiKey) {
            case PRODUCE -> {
                String topic = readString(d);
                int partition = d.readInt();
                int recCount = d.readInt();
                List<Record> records = new ArrayList<>(recCount);
                for (int i = 0; i < recCount; i++) {
                    byte[] key = readBytes(d);
                    byte[] value = readBytes(d);
                    records.add(new Record(key, value, 0L)); // timestamp not on the wire
                }
                yield new ProduceRequest(topic, partition, records);
            }
            case FETCH -> {
                String topic = readString(d);
                int partition = d.readInt();
                long fetchOffset = d.readLong();
                int maxBytes = d.readInt();
                yield new FetchRequest(topic, partition, fetchOffset, maxBytes);
            }
        };
        return new DecodedRequest(apiKey, correlationId, body);
    }

    // ===================== Responses =====================
    // Response header is correlationId only (no apiKey); the client knows the type
    // from the request it sent, so it calls the matching decode* method.

    public static byte[] encodeResponse(int correlationId, ProduceResponse resp) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(buf);
        d.writeInt(correlationId);
        d.writeShort(resp.errorCode().code());
        d.writeLong(resp.baseOffset());
        return buf.toByteArray();
    }

    public static byte[] encodeResponse(int correlationId, FetchResponse resp) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(buf);
        d.writeInt(correlationId);
        d.writeShort(resp.errorCode().code());
        d.writeLong(resp.highWatermark());
        d.writeInt(resp.records().size());
        for (StoredRecord r : resp.records()) {
            d.writeLong(r.offset());
            d.writeLong(r.timestamp());
            writeBytes(d, r.key());
            writeBytes(d, r.value());
        }
        return buf.toByteArray();
    }

    public static DecodedResponse decodeProduceResponse(byte[] payload) throws IOException {
        DataInputStream d = new DataInputStream(new ByteArrayInputStream(payload));
        int correlationId = d.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(d.readShort());
        long baseOffset = d.readLong();
        return new DecodedResponse(correlationId, new ProduceResponse(errorCode, baseOffset));
    }

    public static DecodedResponse decodeFetchResponse(byte[] payload) throws IOException {
        DataInputStream d = new DataInputStream(new ByteArrayInputStream(payload));
        int correlationId = d.readInt();
        ErrorCode errorCode = ErrorCode.fromCode(d.readShort());
        long highWatermark = d.readLong();
        int recCount = d.readInt();
        List<StoredRecord> records = new ArrayList<>(recCount);
        for (int i = 0; i < recCount; i++) {
            long offset = d.readLong();
            long timestamp = d.readLong();
            byte[] key = readBytes(d);
            byte[] value = readBytes(d);
            records.add(new StoredRecord(offset, timestamp, key, value));
        }
        return new DecodedResponse(correlationId, new FetchResponse(errorCode, highWatermark, records));
    }

    // ===================== Field codecs =====================

    /** String = len(int16) + utf8. */
    private static void writeString(DataOutputStream d, String s) throws IOException {
        byte[] b = s.getBytes(UTF_8);
        d.writeShort(b.length);
        d.write(b);
    }

    private static String readString(DataInputStream d) throws IOException {
        int len = d.readUnsignedShort();
        byte[] b = new byte[len];
        d.readFully(b);
        return new String(b, UTF_8);
    }

    /** Length-prefixed byte block; len(int32, -1 = null). */
    private static void writeBytes(DataOutputStream d, byte[] b) throws IOException {
        if (b == null) {
            d.writeInt(-1);
        } else {
            d.writeInt(b.length);
            d.write(b);
        }
    }

    private static byte[] readBytes(DataInputStream d) throws IOException {
        int len = d.readInt();
        if (len < 0) return null;
        byte[] b = new byte[len];
        d.readFully(b);
        return b;
    }
}
