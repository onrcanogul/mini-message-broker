package minibroker.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * A partition's log. Public API (append / read / endOffset) is unchanged from v0;
 * the file handling now lives in LogSegment.
 *
 * v1 Part 1: a thin shell over a SINGLE segment (base offset 0). Rolling over to
 * multiple segments is Part 2.
 *
 * The constructor still takes a path for backward compatibility; only its PARENT
 * directory matters — that's the partition directory the segment file lives in.
 * (The file name itself is now chosen by LogSegment from the base offset.)
 */
public class Log implements AutoCloseable {

    private final LogSegment segment;

    public Log(Path partitionFile) throws IOException {
        Path dir = partitionFile.toAbsolutePath().getParent();
        this.segment = new LogSegment(dir, 0L);
    }

    /** Appends to the end, assigns the offset and returns it. */
    public long append(Record record) throws IOException {
        return segment.append(record);
    }

    /** Reads sequentially from fetchOffset up to ~maxBytes. */
    public List<StoredRecord> read(long fetchOffset, int maxBytes) throws IOException {
        return segment.read(fetchOffset, maxBytes);
    }

    /** Log End Offset: the next offset that will be written. */
    public long endOffset() {
        return segment.endOffset();
    }

    @Override
    public void close() throws IOException {
        segment.close();
    }
}
