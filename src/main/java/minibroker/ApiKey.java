package minibroker;

/** Identifies the request type. Travels in the request header (int16). */
public enum ApiKey {
    PRODUCE(0),
    FETCH(1);

    private final short id;

    ApiKey(int id) {
        this.id = (short) id;
    }

    public short id() {
        return id;
    }

    public static ApiKey fromId(int id) {
        for (ApiKey k : values()) {
            if (k.id == id) return k;
        }
        throw new IllegalArgumentException("Unknown apiKey: " + id);
    }
}
