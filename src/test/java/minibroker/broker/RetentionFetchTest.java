package minibroker.broker;

import minibroker.protocol.ApiKey;
import minibroker.protocol.ErrorCode;
import minibroker.protocol.FetchRequest;
import minibroker.protocol.FetchResponse;
import minibroker.protocol.Protocol;
import minibroker.storage.Log;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RetentionFetchTest {

    private static FetchResponse fetch(RequestHandler handler, long offset) throws Exception {
        Protocol.DecodedRequest req = new Protocol.DecodedRequest(
                ApiKey.FETCH, 1, new FetchRequest("orders", 0, offset, 1 << 20));
        return (FetchResponse) handler.handle(req);
    }

    @Test
    void fetchBelowLogStartOffsetIsOutOfRange(@TempDir Path dataDir) throws Exception {
        // Tiny segments + small byte retention so old segments get deleted.
        LogManager logManager = new LogManager(dataDir, 80, 4096, Config.RETENTION_OFF, 200);
        RequestHandler handler = new RequestHandler(logManager);

        Log log = logManager.getOrCreate("orders", 0);
        for (int i = 0; i < 10; i++) {
            log.append(new minibroker.storage.Record(("k" + i).getBytes(), ("v" + i).getBytes(), 0));
        }
        log.enforceRetention();

        long start = log.logStartOffset();
        assertTrue(start > 0, "retention advanced logStartOffset");

        // A deleted offset (0) -> OFFSET_OUT_OF_RANGE.
        assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, fetch(handler, 0).errorCode());

        // The new oldest offset is still readable -> OK.
        assertEquals(ErrorCode.OK, fetch(handler, start).errorCode());
    }
}
