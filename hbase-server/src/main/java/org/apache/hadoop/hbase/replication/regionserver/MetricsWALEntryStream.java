package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * Same as {@link WALEntryStream} but updates replication source metrics.
 *
 */
public class MetricsWALEntryStream extends WALEntryStream {

  private MetricsSource metrics;

  public MetricsWALEntryStream(PriorityBlockingQueue<Path> logQueue, FileSystem fs, Configuration conf,
      long startPosition, WALEntryFilter filter, MetricsSource metrics) throws IOException  {
    super(logQueue, fs, conf, startPosition, filter);
    this.metrics = metrics;
  }

  @Override
  protected void dequeueCurrentLog() throws IOException {
      super.dequeueCurrentLog();
      metrics.decrSizeOfLogQueue();
  }

  @Override
  protected Entry filterEntry(Entry entry) {
    Entry filtered = super.filterEntry(entry);
    if (entry != null && filtered == null) {
      metrics.incrLogEditsFiltered();
    }
    return filtered;
  }

  @Override
  protected Entry readNextEntry() throws IOException {
    Entry nextEntry = super.readNextEntry();
    if (nextEntry != null) metrics.incrLogEditsRead();
    return nextEntry;
  }

}
