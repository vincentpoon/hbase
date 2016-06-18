/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream.WALEntryStreamRuntimeException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * Reads and filters WAL entries, groups the filtered entries into batches, and puts the batches onto a queue
 *
 */
@InterfaceAudience.Private
public class ReplicationWALEntryBatcher extends Thread {
  private static final Log LOG = LogFactory.getLog(ReplicationSource.class);

  private PriorityBlockingQueue<Path> logQueue;
  private FileSystem fs;
  private Configuration conf;
  private BlockingQueue<WALEntryBatch> entryBatchQueue;
  // max (heap) size of each batch - multiply by number of batches in queue to get total
  private long replicationBatchSizeCapacity;
  // max count of each batch - multiply by number of batches in queue to get total
  private int replicationBatchNbCapacity;
  // position in the WAL to start reading at
  private long currentPosition;
  private WALEntryFilter filter;
  private long sleepForRetries;
  //Indicates whether this particular worker is running
  private boolean isBatcherRunning = true;
  private ReplicationQueueInfo replicationQueueInfo;
  private int maxRetriesMultiplier;
  private MetricsSource metrics;

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
      FileSystem fs, Configuration conf, WALEntryFilter filter, MetricsSource metrics) {
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
    this.maxRetriesMultiplier =
        this.conf.getInt("replication.source.maxretriesmultiplier", 300); // 5 minutes @ 1 sec per
    this.metrics = metrics;
    // default to 1 batch for similar behavior as old (no queue/batches) implementation
    int batchCount = conf.getInt("replication.source.nb.batches", 1);
    this.entryBatchQueue = new LinkedBlockingQueue<>(batchCount);
  }

  @Override
  public void run() {
    int sleepMultiplier = 1;
    while (isBatcherRunning()) { // we only loop back here if something fatal happened to our stream
      try (WALEntryStream entryStream =
          new MetricsWALEntryStream(logQueue, fs, conf, currentPosition, filter, metrics)) {
        while (isBatcherRunning()) { // loop here to keep reusing stream while we can
          WALEntryBatch batch = null;
          while (entryStream.hasNext()) {
            if (batch == null)
              batch = new WALEntryBatch(replicationBatchNbCapacity, entryStream.getCurrentPath());
            Entry entry = entryStream.next();
            batch.addEntry(entry);
            updateBatchStats(batch, entry, entryStream.getPosition());
            // Stop if too many entries or too big
            // FIXME check the relationship between single wal group and overall
            if (batch.getSize() >= replicationBatchSizeCapacity
                || batch.getNbEntries() >= replicationBatchNbCapacity) {
              break;
            }
          }
          metrics.incrLogReadInBytes(entryStream.getPosition() - currentPosition);
          currentPosition = entryStream.getPosition();
          if (batch != null
              && (batch.getNbEntries() > 0 || currentPosition != entryStream.getPosition())) {
            LOG.trace(
              String.format("Read %s WAL entries eligible for replication", batch.getNbEntries()));
            entryBatchQueue.put(batch);
            sleepMultiplier = 1;
          } else { // got no entries and didn't advance position in WAL
            LOG.trace("Didn't read any new entries from WAL");
            if (replicationQueueInfo.isQueueRecovered()) {
              // we're done with queue recovery, shut ourself down
              setBatcherRunning(false);
            } else {
              Thread.sleep(sleepForRetries);
            }
          }
          entryStream.reset(); // reuse stream
        }
      } catch (IOException | WALEntryStreamRuntimeException e) { // stream related
        LOG.warn("Failed to read stream of replication entries", e);
        if (sleepMultiplier < maxRetriesMultiplier) sleepMultiplier++;
        Threads.sleep(sleepForRetries * sleepMultiplier);
      } catch (InterruptedException e) {
        LOG.trace("Interrupted while sleeping between WAL reads");
        Thread.currentThread().interrupt();
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

  private void updateBatchStats(WALEntryBatch batch, Entry entry, long entryPosition) {
    WALEdit edit = entry.getEdit();
    if (edit != null && edit.size() != 0) {
      batch.incrementHeapSize(edit.heapSize());
      batch.incrementHeapSize(calculateTotalSizeOfStoreFiles(edit));
      Pair<Integer, Integer> nbRowsAndHFiles = countDistinctRowKeysAndHFiles(edit);
      batch.incrementNbRowKeys(nbRowsAndHFiles.getFirst());
      batch.incrementNbHFiles(nbRowsAndHFiles.getSecond());
    }
    batch.lastWalPosition = entryPosition;
  }

  /**
   * Count the number of different row keys in the given edit because of mini-batching. We assume
   * that there's at least one Cell in the WALEdit.
   * @param edit edit to count row keys from
   * @return number of different row keys and HFiles
   */
  private Pair<Integer, Integer> countDistinctRowKeysAndHFiles(WALEdit edit) {
    List<Cell> cells = edit.getCells();
    int distinctRowKeys = 1;
    int totalHFileEntries = 0;
    Cell lastCell = cells.get(0);

    int totalCells = edit.size();
    for (int i = 0; i < totalCells; i++) {
      // Count HFiles to be replicated
      if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
        try {
          BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
          List<StoreDescriptor> stores = bld.getStoresList();
          int totalStores = stores.size();
          for (int j = 0; j < totalStores; j++) {
            totalHFileEntries += stores.get(j).getStoreFileList().size();
          }
        } catch (IOException e) {
          LOG.error("Failed to deserialize bulk load entry from wal edit. "
              + "Then its hfiles count will not be added into metric.");
        }
      }

      if (!CellUtil.matchingRows(cells.get(i), lastCell)) {
        distinctRowKeys++;
      }
      lastCell = cells.get(i);
    }

    Pair<Integer, Integer> result = new Pair<>(distinctRowKeys, totalHFileEntries);
    return result;
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
              + "request size calculation.",
            e);
        }
      }
    }
    return totalStoreFilesSize;
  }

  /**
   * @return whether the batcher is running
   */
  public boolean isBatcherRunning() {
    return isBatcherRunning && !isInterrupted();
  }

  /**
   * @param batcherRunning the batcherRunning to set
   */
  public void setBatcherRunning(boolean batcherRunning) {
    this.isBatcherRunning = batcherRunning;
  }

  /**
   * Holds a batch of WAL entries to replicate, along with some statistics
   *
   */
  public static class WALEntryBatch {
    private List<Entry> walEntries;
    // last WAL that was read
    private Path lastWalPath;
    // position in WAL of last entry in this batch
    private long lastWalPosition = 0;
    // number of distinct row keys in this batch
    private int nbRowKeys = 0;
    // number of HFiles
    private int nbHFiles = 0;
    // heap size of data we need to replicate
    private long heapSize = 0;

    /**
     * @param walEntries
     * @param lastWalPath Path of the WAL the last entry in this batch was read from
     * @param lastWalPosition Position in the WAL the last entry in this batch was read from
     */
    private WALEntryBatch(int maxNbEntries, Path lastWalPath) {
      this.walEntries = new ArrayList<>(maxNbEntries);
      this.lastWalPath = lastWalPath;
    }

    public void addEntry(Entry entry) {
      walEntries.add(entry);
    }

    /**
     * @return the WAL Entries.
     */
    public List<Entry> getWalEntries() {
      return walEntries;
    }

    /**
     * @return the path of the last WAL that was read.
     */
    public Path getLastWalPath() {
      return lastWalPath;
    }

    /**
     * @return the position in the last WAL that was read.
     */
    public long getLastWalPosition() {
      return lastWalPosition;
    }

    public int getNbEntries() {
      return walEntries.size();
    }

    /**
     * @return the number of distinct row keys in this batch
     */
    public int getNbRowKeys() {
      return nbRowKeys;
    }

    /**
     * @return the number of HFiles in this batch
     */
    public int getNbHFiles() {
      return nbHFiles;
    }

    /**
     * @return total number of operations in this batch
     */
    public int getNbOperations() {
      return getNbRowKeys() + getNbHFiles();
    }

    /**
     * @return the heap size of this batch
     */
    public long getSize() {
      return heapSize;
    }

    private void incrementNbRowKeys(int increment) {
      nbRowKeys += increment;
    }

    private void incrementNbHFiles(int increment) {
      nbHFiles += increment;
    }

    private void incrementHeapSize(long increment) {
      heapSize += increment;
    }
  }
}
