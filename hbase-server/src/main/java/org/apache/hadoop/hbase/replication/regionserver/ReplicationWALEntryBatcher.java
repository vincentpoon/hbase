package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

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
 * Reads and filters batches of WAL entries into a queue 
 *
 */
public class ReplicationWALEntryBatcher extends Thread {
  private static final Log LOG = LogFactory.getLog(ReplicationSource.class);
  
  private PriorityBlockingQueue<Path> logQueue;
  private FileSystem fs;
  private Configuration conf;
  // entry batches paired with their log position after reading the batch
  BlockingQueue<Pair<List<Entry>, Long>> entryBatchQueue;
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

  public ReplicationWALEntryBatcher(PriorityBlockingQueue<Path> logQueue, long startPosition,
      FileSystem fs, Configuration conf, BlockingQueue<Pair<List<Entry>, Long>> entryBatchQueue, WALEntryFilter filter) {
    this.logQueue = logQueue;
    this.currentPosition = startPosition;
    this.fs = fs;
    this.conf = conf;
    this.filter = filter;
    this.entryBatchQueue = entryBatchQueue;
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

  private Pair<List<Entry>, Long> readBatch() throws IOException {       
    try (WALEntryStream entryStream = new WALEntryStream(logQueue, fs, conf, this.currentPosition, filter)) {
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
