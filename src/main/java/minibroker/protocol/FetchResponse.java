package minibroker.protocol;

import minibroker.storage.StoredRecord;

import java.util.List;

/**
 * Body: errorCode(int16) | highWatermark(int64) | recCount(int32) |
 *       recCount × [ offset | timestamp | keyLen | key | valLen | value ]
 *
 * highWatermark = highest offset a consumer is allowed to read up to (here: the LEO).
 */
public record FetchResponse(ErrorCode errorCode, long highWatermark, List<StoredRecord> records) {
}
