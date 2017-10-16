package inputoutput;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Notifies a worker when all the chunks of a file have been processed,
 * and are ready to be merged and stored.
 *
 * Created by gina on 2/28/17.
 */
public class Tracker {

    private AtomicInteger chunksSubmitted;
    private AtomicInteger chunksProcessed;
    private AtomicBoolean doneReading;
    private AtomicBoolean failed;

    public Tracker() {
        this.chunksSubmitted = new AtomicInteger(0);
        this.chunksProcessed = new AtomicInteger(0);
        this.doneReading = new AtomicBoolean(false);
        this.failed = new AtomicBoolean(false);
    }

    public int getChunksSubmitted() { return chunksSubmitted.get(); }

    public int submitChunk() {
        return chunksSubmitted.getAndIncrement();
    }

    public int processChunk() {
        return chunksProcessed.getAndIncrement();
    }

    public void finishReading() {
        doneReading.set(true);
    }

    public void fail() {
        failed.set(true);
    }

    public boolean isFailed() {
        return failed.get();
    }

    public boolean isDoneProcessing() {
        return chunksProcessed.get() == chunksSubmitted.get() && doneReading.compareAndSet(true, false);
    }
}
