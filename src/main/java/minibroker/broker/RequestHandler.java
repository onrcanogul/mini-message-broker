package minibroker.broker;

import minibroker.protocol.ErrorCode;
import minibroker.protocol.FetchRequest;
import minibroker.protocol.FetchResponse;
import minibroker.protocol.ProduceRequest;
import minibroker.protocol.ProduceResponse;
import minibroker.protocol.Protocol;
import minibroker.storage.Log;
import minibroker.storage.Record;
import minibroker.storage.StoredRecord;

import java.io.IOException;
import java.util.List;

/**
 * Pure request logic: decoded request -> response object. Knows nothing about
 * sockets, so it can be unit-tested in memory. BrokerServer wraps it with transport.
 */
public class RequestHandler {

    private final LogManager logManager;

    public RequestHandler(LogManager logManager) {
        this.logManager = logManager;
    }

    /** Dispatches by apiKey; returns a ProduceResponse or FetchResponse. */
    public Object handle(Protocol.DecodedRequest req) throws IOException {
        return switch (req.apiKey()) {
            case PRODUCE -> handleProduce((ProduceRequest) req.body());
            case FETCH -> handleFetch((FetchRequest) req.body());
        };
    }

    private ProduceResponse handleProduce(ProduceRequest req) throws IOException {
        if (!logManager.isValidPartition(req.partition())) {
            return new ProduceResponse(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, -1);
        }
        Log log = logManager.getOrCreate(req.topic(), req.partition());
        long baseOffset = -1;
        for (Record r : req.records()) {
            // The wire didn't carry a timestamp; the broker stamps at append time.
            Record stamped = new Record(r.key(), r.value(), System.currentTimeMillis());
            long offset = log.append(stamped);
            if (baseOffset < 0) baseOffset = offset; // offset of the first record in the batch
        }
        return new ProduceResponse(ErrorCode.OK, baseOffset);
    }

    private FetchResponse handleFetch(FetchRequest req) throws IOException {
        if (!logManager.isValidPartition(req.partition())) {
            return new FetchResponse(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, -1, List.of());
        }
        Log log = logManager.get(req.topic(), req.partition());
        if (log == null) { // never produced to -> topic/partition doesn't exist yet
            return new FetchResponse(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, -1, List.of());
        }

        long leo = log.endOffset();
        long fetchOffset = req.fetchOffset();
        if (fetchOffset < 0 || fetchOffset > leo) {
            return new FetchResponse(ErrorCode.OFFSET_OUT_OF_RANGE, leo, List.of());
        }
        if (fetchOffset == leo) { // caller is caught up: no new data yet, not an error
            return new FetchResponse(ErrorCode.OK, leo, List.of());
        }
        List<StoredRecord> records = log.read(fetchOffset, req.maxBytes());
        return new FetchResponse(ErrorCode.OK, leo, records);
    }
}
