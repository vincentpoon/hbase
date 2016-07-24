package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream.WALEntryStreamRuntimeException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;

// we can probably change this once we have streaming output in addition to input
// but for now, our replicate() takes a batch, so that's what we create here
/**
 * Reads and filters WAL entries in batches, and puts the batches on a queue
 *
 */
public class ReplicationWALEntryBatcher extends Thread {
  private static final Log LOG = LogFactory.getLog(ReplicationSource.class);
  
  private PriorityBlockingQueue<Path> logQueue;
  private FileSystem fs;
  private Configuration conf;
  // entry batches paired with the ending log position of the batch
  private BlockingQueue<Pair<List<Entry>, Long>> entryBatchQueue = new LinkedBlockingQueue<>(1);
  // max size of each batch - multiply by number of batches to get total
  private long replicationBatchSizeCapacity;
  // max count of each batch - multiply by number of batches to get total
  private int replicationBatchNbCapacity;
  // position in the WAL to start reading at
  private long currentPosition;
  private WALEntryFilter filter;
  private long sleepForRetries;
  //Indicates whether this particular worker is running
  private boolean workerRunning = true;

  /**
   * Creates a batcher for a given WAL queue.
   * Reads WAL entries off a given queue, batches the entries, and puts them on a batch queue.
   * @param logQueue The replication WAL queue
   * @param startPosition position in the first WAL to start reading from
   * @param fs
   * @param conf
   * @param entryBatchQueue The queue of entry batches that this batcher will write to
   * @param filter The filter to use while reading
   */
  public ReplicationWALEntryBatcher(PriorityBlockingQueue<Path> logQueue, long startPosition,
      FileSystem fs, Configuration conf, WALEntryFilter filter) {
    this.logQueue = logQueue;
    this.currentPosition = startPosition;
    this.fs = fs;
    this.conf = conf;
    this.filter = filter;
    this.replicationBatchSizeCapacity =
        this.conf.getLong("replication.source.size.capacity", 1024 * 1024 * 64);
    this.replicationBatchNbCapacity = this.conf.getInt("replication.source.nb.capacity", 25000);
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);    // 1 second
  }

  @Override
  public void run() {
    while (isWorkerRunning()) {
      try {
        Pair<List<Entry>, Long> batch = readBatch();
        if (batch != null) {
          entryBatchQueue.put(batch);
        } else {
          Thread.sleep(sleepForRetries);
        }
      } catch (IOException | WALEntryStreamRuntimeException e) {
        LOG.warn("Failed to read replication entry batch", e);
        Threads.sleep(sleepForRetries);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while reading replication entry batch", e);
      }      
    }
  }
  
  /**
   * Retrieves the next batch of WAL entries from the queue, waiting up to the specified time for a batch to become available
   * @return A batch of entries, along with the position in the log after reading the batch
   * @throws InterruptedException if interrupted while waiting
   */
  public Pair<List<Entry>, Long> poll(long timeout, TimeUnit unit) throws InterruptedException {
    return entryBatchQueue.poll(timeout, unit);
  }

  private Pair<List<Entry>, Long> readBatch() throws IOException {       
    try (WALEntryStream entryStream = new WALEntryStream(null, logQueue, fs, conf, this.currentPosition, filter)) {
      List<Entry> batch = new ArrayList<>(1);
      int currentSize = 0;
      while (entryStream.hasNext()) {
        Entry entry = entryStream.next();        
        currentSize += entry.getEdit().heapSize();
        batch.add(entry);
        // Stop if too many entries or too big
        // FIXME check the relationship between single wal group and overall
        if (currentSize >= replicationBatchNbCapacity
            || batch.size() >= replicationBatchSizeCapacity) {
          break;
        }
      }
      
      Pair<List<Entry>, Long> batchPair = null;      
      if (batch.size() > 0 || this.currentPosition != entryStream.getPosition()) {
        batchPair = new Pair<>(batch, entryStream.getPosition());        
      }
      this.currentPosition = entryStream.getPosition();
      return batchPair;      
    }
  }

  /**
   * @return the workerRunning
   */
  public boolean isWorkerRunning() {
    return this.workerRunning;
  }

  /**
   * @param workerRunning the workerRunning to set
   */
  public void setWorkerRunning(boolean workerRunning) {
    this.workerRunning = workerRunning;
  }
}
