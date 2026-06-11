package minibroker;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class ProtocolTest {

    // Records hold byte[], so record auto-equals (reference equality on arrays) is useless.
    // We compare field by field with assertArrayEquals instead.
    private static void assertSameRecord(Record expected, Record actual) {
        assertArrayEquals(expected.key(), actual.key());
        assertArrayEquals(expected.value(), actual.value());
    }

    private static void assertSameStored(StoredRecord expected, StoredRecord actual) {
        assertEquals(expected.offset(), actual.offset());
        assertEquals(expected.timestamp(), actual.timestamp());
        assertArrayEquals(expected.key(), actual.key());
        assertArrayEquals(expected.value(), actual.value());
    }

    @Test
    void produceRequestRoundTrip() throws Exception {
        ProduceRequest req = new ProduceRequest("orders", 0, List.of(
                new Record("k0".getBytes(UTF_8), "v0".getBytes(UTF_8), 0),
                new Record(null,                "v1".getBytes(UTF_8), 0)));

        byte[] payload = Protocol.encodeRequest(42, req);
        Protocol.DecodedRequest dec = Protocol.decodeRequest(payload);

        assertEquals(ApiKey.PRODUCE, dec.apiKey());
        assertEquals(42, dec.correlationId());
        ProduceRequest out = (ProduceRequest) dec.body();
        assertEquals("orders", out.topic());
        assertEquals(0, out.partition());
        assertEquals(2, out.records().size());
        assertSameRecord(req.records().get(0), out.records().get(0));
        assertSameRecord(req.records().get(1), out.records().get(1)); // null key survives
    }

    @Test
    void produceResponseRoundTrip() throws Exception {
        ProduceResponse resp = new ProduceResponse(ErrorCode.OK, 7);

        byte[] payload = Protocol.encodeResponse(99, resp);
        Protocol.DecodedResponse dec = Protocol.decodeProduceResponse(payload);

        assertEquals(99, dec.correlationId());
        ProduceResponse out = (ProduceResponse) dec.body();
        assertEquals(ErrorCode.OK, out.errorCode());
        assertEquals(7, out.baseOffset());
    }

    @Test
    void fetchRequestRoundTrip() throws Exception {
        FetchRequest req = new FetchRequest("orders", 3, 100L, 1 << 20);

        byte[] payload = Protocol.encodeRequest(5, req);
        Protocol.DecodedRequest dec = Protocol.decodeRequest(payload);

        assertEquals(ApiKey.FETCH, dec.apiKey());
        assertEquals(5, dec.correlationId());
        FetchRequest out = (FetchRequest) dec.body();
        assertEquals("orders", out.topic());
        assertEquals(3, out.partition());
        assertEquals(100L, out.fetchOffset());
        assertEquals(1 << 20, out.maxBytes());
    }

    @Test
    void fetchResponseRoundTrip() throws Exception {
        FetchResponse resp = new FetchResponse(ErrorCode.OK, 2, List.of(
                new StoredRecord(0, 1000, "k0".getBytes(UTF_8), "v0".getBytes(UTF_8)),
                new StoredRecord(1, 1001, null,                "v1".getBytes(UTF_8))));

        byte[] payload = Protocol.encodeResponse(11, resp);
        Protocol.DecodedResponse dec = Protocol.decodeFetchResponse(payload);

        assertEquals(11, dec.correlationId());
        FetchResponse out = (FetchResponse) dec.body();
        assertEquals(ErrorCode.OK, out.errorCode());
        assertEquals(2, out.highWatermark());
        assertEquals(2, out.records().size());
        assertSameStored(resp.records().get(0), out.records().get(0));
        assertSameStored(resp.records().get(1), out.records().get(1)); // null key survives
    }

    @Test
    void frameWriteReadOverPipe() throws Exception {
        byte[] payload = Protocol.encodeRequest(1, new FetchRequest("t", 0, 0L, 64));

        PipedInputStream in = new PipedInputStream(4096);
        PipedOutputStream out = new PipedOutputStream(in);

        // Write the frame from a separate thread (avoids any pipe-buffer blocking).
        Thread writer = new Thread(() -> {
            try {
                Protocol.writeFrame(out, payload);
                out.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        writer.start();

        // Read the length prefix explicitly to verify frameLen, then the payload.
        DataInputStream din = new DataInputStream(in);
        int frameLen = din.readInt();
        assertEquals(payload.length, frameLen, "frameLen must equal payload length");
        byte[] got = new byte[frameLen];
        din.readFully(got);
        assertArrayEquals(payload, got);

        writer.join();

        // And the bytes still decode back into the original request.
        Protocol.DecodedRequest dec = Protocol.decodeRequest(got);
        assertEquals(ApiKey.FETCH, dec.apiKey());
        assertEquals(1, dec.correlationId());
    }
}
