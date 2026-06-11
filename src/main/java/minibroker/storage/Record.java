package minibroker.storage;

/**
 * Raw record sent by the producer. NO offset here — the offset is assigned by Log (during append).
 * key may be null; value is required. The caller provides the timestamp for now.
 */
public record Record(byte[] key, byte[] value, long timestamp) {
}
