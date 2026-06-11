package minibroker;

/** Body: errorCode(int16) | baseOffset(int64). baseOffset = offset of the first appended record. */
public record ProduceResponse(ErrorCode errorCode, long baseOffset) {
}
