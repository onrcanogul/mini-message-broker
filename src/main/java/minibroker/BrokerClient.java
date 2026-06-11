package minibroker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

/**
 * Thin client over the wire protocol — keeps tests (and manual experiments) readable.
 * One connection, sequential request/response, with a self-incrementing correlationId
 * that we verify on every reply.
 *
 * v0: partition is always 0.
 */
public class BrokerClient implements AutoCloseable {

    /** A fetch result paired with the broker's high watermark. */
    public record FetchResult(List<StoredRecord> records, long highWatermark) {
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private int correlationId = 0;

    private BrokerClient(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedInputStream(socket.getInputStream());
        this.out = new BufferedOutputStream(socket.getOutputStream());
    }

    public static BrokerClient connect(String host, int port) throws IOException {
        return new BrokerClient(new Socket(host, port));
    }

    /** Appends records to (topic, partition 0); returns the offset of the first record. */
    public long produce(String topic, List<Record> records) throws IOException {
        int corr = ++correlationId;
        Protocol.writeFrame(out, Protocol.encodeRequest(corr, new ProduceRequest(topic, 0, records)));
        Protocol.DecodedResponse resp = Protocol.decodeProduceResponse(Protocol.readFrame(in));
        expectCorrelation(corr, resp.correlationId());
        ProduceResponse body = (ProduceResponse) resp.body();
        if (body.errorCode() != ErrorCode.OK) {
            throw new IOException("produce failed: " + body.errorCode());
        }
        return body.baseOffset();
    }

    /** Fetches from (topic, partition 0) starting at fetchOffset. */
    public FetchResult fetch(String topic, long fetchOffset, int maxBytes) throws IOException {
        int corr = ++correlationId;
        Protocol.writeFrame(out, Protocol.encodeRequest(corr, new FetchRequest(topic, 0, fetchOffset, maxBytes)));
        Protocol.DecodedResponse resp = Protocol.decodeFetchResponse(Protocol.readFrame(in));
        expectCorrelation(corr, resp.correlationId());
        FetchResponse body = (FetchResponse) resp.body();
        if (body.errorCode() != ErrorCode.OK) {
            throw new IOException("fetch failed: " + body.errorCode());
        }
        return new FetchResult(body.records(), body.highWatermark());
    }

    private static void expectCorrelation(int expected, int actual) throws IOException {
        if (expected != actual) {
            throw new IOException("correlationId mismatch: expected " + expected + ", got " + actual);
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
