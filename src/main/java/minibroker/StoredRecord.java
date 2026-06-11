package minibroker;

/**
 * A record AFTER it has been written to the Log: it now has an offset.
 * read() returns this; the difference from Record is that the offset field is populated.
 */
public record StoredRecord(long offset, long timestamp, byte[] key, byte[] value) {
}
