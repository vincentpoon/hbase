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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream.WALEntryStreamRuntimeException;
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
  private BlockingQueue<WALEntryBatch> entryBatchQueue = new LinkedBlockingQueue<>(1);
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
  private ReplicationQueueInfo replicationQueueInfo;
  
  public static class WALEntryBatch {
    private List<Entry> walEntries;
    // last WAL that was read
    private Path lastWalPath;
    // position in WAL of last entry in this batch
    private long lastWalPosition;
    // Current size of data we need to replicate
    private int size = 0;
    
    /**
     * @param walEntries
     * @param lastWalPath
     * @param lastWalPosition
     * @param size
     */
    public WALEntryBatch(List<Entry> walEntries, Path lastWalPath, long lastWalPosition, int size) {
      this.walEntries = walEntries;
      this.lastWalPath = lastWalPath;
      this.lastWalPosition = lastWalPosition;
      this.size = size;
    }

    /**
     * @return Returns the walEntries.
     */
    public List<Entry> getWalEntries() {
      return walEntries;
    }

    /**
     * @return Returns the path of the last WAL that was read.
     */
    public Path getLastWalPath() {
      return lastWalPath;
    }

    /**
     * @return Returns the position in the last WAL that was read.
     */
    public long getLastWalPosition() {
      return lastWalPosition;
    }

    /**
     * @return the currentSize
     */
    public int getSize() {
      return size;
    }

  }

  /**
   * Creates a batcher for a given WAL queue.
   * Reads WAL entries off a given queue, batches the entries, and puts them on a batch queue.
   * @param replicationQueueInfo 
   * @param logQueue The WAL queue to read off of
   * @param startPosition position in the first WAL to start reading from
   * @param fs
   * @param conf
   * @param entryBatchQueue The queue of entry batches that this batcher will write to
   * @param filter The filter to use while reading
   */
  public ReplicationWALEntryBatcher(ReplicationQueueInfo replicationQueueInfo, PriorityBlockingQueue<Path> logQueue, long startPosition,
      FileSystem fs, Configuration conf, WALEntryFilter filter) {
    this.replicationQueueInfo = replicationQueueInfo;
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
    int sleepMultiplier = 1;
    while (isWorkerRunning()) {      
      try {
        WALEntryBatch batch = readBatch();
        if (batch != null) {
          entryBatchQueue.put(batch);
          sleepMultiplier = 1;
        } else {
          LOG.debug("Didn't read any new entries from WAL");
          if (sleepMultiplier < 60) sleepMultiplier++; //TODO remove
        }
        Thread.sleep(sleepForRetries * sleepMultiplier);
      } catch (IOException | WALEntryStreamRuntimeException e) {
        LOG.warn("Failed to read replication entry batch", e);
        Threads.sleep(sleepForRetries * sleepMultiplier);
      } catch (InterruptedException e) {
        LOG.trace("Interrupted while reading replication entry batch");
      }      
    }
  }
  
  /**
   * Retrieves the next batch of WAL entries from the queue, waiting up to the specified time for a batch to become available
   * @return A batch of entries, along with the position in the log after reading the batch
   * @throws InterruptedException if interrupted while waiting
   */
  public WALEntryBatch poll(long timeout, TimeUnit unit) throws InterruptedException {
    return entryBatchQueue.poll(timeout, unit);
  }
  
  public WALEntryBatch take() throws InterruptedException {
    return entryBatchQueue.take();
  }

  private WALEntryBatch readBatch() throws IOException {   
    try (WALEntryStream entryStream = new WALEntryStream(replicationQueueInfo, logQueue, fs, conf, this.currentPosition, filter)) {
      List<Entry> entries = new ArrayList<>(replicationBatchNbCapacity);
      int currentSize = 0;
      while (entryStream.hasNext()) {
        Entry entry = entryStream.next();        
        currentSize += entry.getEdit().heapSize();
        currentSize += calculateTotalSizeOfStoreFiles(entry.getEdit());
        entries.add(entry);        
        // Stop if too many entries or too big
        // FIXME check the relationship between single wal group and overall
        if (currentSize >= replicationBatchNbCapacity
            || entries.size() >= replicationBatchSizeCapacity) {
          break;
        }
      }
      WALEntryBatch batch = null;
      currentPosition = entryStream.getPosition();
      if (entries.size() > 0 || this.currentPosition != entryStream.getPosition()) {
        batch = new WALEntryBatch(entries, entryStream.getCurrentPath(), currentPosition, currentSize);
      }
      return batch;      
    }
  }
  
  /**
   * Calculate the total size of all the store files
   * @param edit edit to count row keys from
   * @return the total size of the store files
   */
  private int calculateTotalSizeOfStoreFiles(WALEdit edit) {
    List<Cell> cells = edit.getCells();
    int totalStoreFilesSize = 0;

    int totalCells = edit.size();
    for (int i = 0; i < totalCells; i++) {
      if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
        try {
          BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
          List<StoreDescriptor> stores = bld.getStoresList();
          int totalStores = stores.size();
          for (int j = 0; j < totalStores; j++) {
            totalStoreFilesSize += stores.get(j).getStoreFileSizeBytes();
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize bulk load entry from wal edit. "
              + "Size of HFiles part of cell will not be considered in replication "
              + "request size calculation.", e);
        }
      }
    }
    return totalStoreFilesSize;
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
