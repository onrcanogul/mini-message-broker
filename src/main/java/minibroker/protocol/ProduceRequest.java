package minibroker.protocol;

import minibroker.storage.Record;

import java.util.List;

/**
 * Body: topic(string) | partition(int32) | recCount(int32) |
 *       recCount × [ keyLen | key | valLen | value ]
 *
 * Records carry only key/value over the wire; timestamp is NOT transmitted
 * (the broker stamps each record at append time).
 */
public record ProduceRequest(String topic, int partition, List<Record> records) {
}
