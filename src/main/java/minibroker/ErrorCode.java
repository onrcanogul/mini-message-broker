package minibroker;

/** Result status carried in every response body (int16). */
public enum ErrorCode {
    OK(0),
    UNKNOWN_TOPIC_OR_PARTITION(1),
    OFFSET_OUT_OF_RANGE(2),
    CORRUPT_REQUEST(3);

    private final short code;

    ErrorCode(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    public static ErrorCode fromCode(int code) {
        for (ErrorCode e : values()) {
            if (e.code == code) return e;
        }
        throw new IllegalArgumentException("Unknown errorCode: " + code);
    }
}
