package minibroker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class BrokerServerTest {

    @Test
    void produceThenFetchOverRealSocket(@TempDir Path dataDir) throws Exception {
        LogManager logManager = new LogManager(dataDir);
        RequestHandler handler = new RequestHandler(logManager);

        try (BrokerServer server = new BrokerServer(new Config(0, dataDir), handler)) {
            server.start();

            try (Socket socket = new Socket("localhost", server.port());
                 InputStream in = new BufferedInputStream(socket.getInputStream());
                 OutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

                // --- PRODUCE: 3 records to topic "orders" partition 0 ---
                ProduceRequest produce = new ProduceRequest("orders", 0, List.of(
                        new Record("k0".getBytes(UTF_8), "v0".getBytes(UTF_8), 0),
                        new Record(null,                "v1".getBytes(UTF_8), 0),
                        new Record("k2".getBytes(UTF_8), "v2".getBytes(UTF_8), 0)));
                Protocol.writeFrame(out, Protocol.encodeRequest(1, produce));

                Protocol.DecodedResponse pResp =
                        Protocol.decodeProduceResponse(Protocol.readFrame(in));
                assertEquals(1, pResp.correlationId());
                ProduceResponse pr = (ProduceResponse) pResp.body();
                assertEquals(ErrorCode.OK, pr.errorCode());
                assertEquals(0, pr.baseOffset(), "first record's offset");

                // --- FETCH from offset 0 ---
                FetchRequest fetch = new FetchRequest("orders", 0, 0L, 1 << 20);
                Protocol.writeFrame(out, Protocol.encodeRequest(2, fetch));

                Protocol.DecodedResponse fResp =
                        Protocol.decodeFetchResponse(Protocol.readFrame(in));
                assertEquals(2, fResp.correlationId());
                FetchResponse fr = (FetchResponse) fResp.body();
                assertEquals(ErrorCode.OK, fr.errorCode());
                assertEquals(3, fr.highWatermark(), "HW = LEO = 3");

                List<StoredRecord> recs = fr.records();
                assertEquals(3, recs.size());
                assertEquals(0, recs.get(0).offset());
                assertArrayEquals("k0".getBytes(UTF_8), recs.get(0).key());
                assertArrayEquals("v0".getBytes(UTF_8), recs.get(0).value());
                assertNull(recs.get(1).key());
                assertArrayEquals("v1".getBytes(UTF_8), recs.get(1).value());
                assertEquals(2, recs.get(2).offset());
                assertArrayEquals("v2".getBytes(UTF_8), recs.get(2).value());
            }
        }
    }
}
