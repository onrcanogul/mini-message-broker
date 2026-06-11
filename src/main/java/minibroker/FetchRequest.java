package minibroker;

/** Body: topic(string) | partition(int32) | fetchOffset(int64) | maxBytes(int32). */
public record FetchRequest(String topic, int partition, long fetchOffset, int maxBytes) {
}
