package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALFactory;

/**
 * Streaming access to WAL entries. This class is given an ordered queue of WAL {@link Path}, and
 * continually iterates through all the WAL {@link Entry} in the queue, handling the rolling over of
 * one log to another.
 */
@InterfaceAudience.Private
public class WALEntryStream implements Iterator<Entry>, AutoCloseable, Iterable<Entry> {
  private Reader reader;
  private Path currentPath;
  private Entry currentEntry;
  // position in the Reader to start reading at
  private long currentPosition = 0;
  private PriorityBlockingQueue<Path> logQueue;
  private FileSystem fs;
  private Configuration conf;
  private WALEntryFilter filter;

  /**
   * Create an entry stream over the given queue
   * @param logQueue the queue of WAL paths
   * @param fs {@link FileSystem} to use to create {@link Reader} for this stream
   * @param conf {@link Configuration} to use to create {@link Reader} for this stream
   * @throws IOException
   */
  public WALEntryStream(PriorityBlockingQueue<Path> logQueue, FileSystem fs, Configuration conf)
      throws IOException {
    this(logQueue, fs, conf, null);
  }
  
  /**
   * Create a filtered entry stream over the given queue
   * @param logQueue the queue of WAL paths
   * @param fs {@link FileSystem} to use to create {@link Reader} for this stream
   * @param conf {@link Configuration} to use to create {@link Reader} for this stream
   * @param filter filter to use on this stream
   * @throws IOException
   */
  public WALEntryStream(PriorityBlockingQueue<Path> logQueue, FileSystem fs, Configuration conf, WALEntryFilter filter) throws IOException {
    this.logQueue = logQueue;
    this.fs = fs;
    this.conf = conf;
    this.filter = filter;
    tryAdvanceEntry();
  }

  /**
   * Returns true if there is another WAL {@link Entry} in the logs
   * @return true if there is another WAL {@link Entry}
   */
  @Override
  public boolean hasNext() {
    return currentEntry != null;
  }

  /**
   * Returns the next WAL entry in this stream
   * @return the next WAL entry in this stream
   * @throws WALEntryStreamRuntimeException if there was an IOException
   * @throws NoSuchElementException if no more entries in the stream.
   */
  @Override
  public Entry next() {
    if (currentEntry == null) throw new NoSuchElementException();
    Entry save = currentEntry;
    try {
      tryAdvanceEntry();
    } catch (IOException e) {
      throw new WALEntryStreamRuntimeException(e);
    }
    return save;
  }

  /**
   * Not supported.
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    closeReader();
  }

  /**
   * Returns an iterator over WAL entries in this queue.
   * @return the iterator over WAL entries in this queue.
   */
  @Override
  public Iterator<Entry> iterator() {
    return this;
  }

  /**
   * Returns the position we stopped reading at
   * @return the position we stopped reading at.
   */
  public long getPosition() {
    return currentPosition;
  }

  /**
   * Set the position to start reading at
   * @param pos
   * @throws IOException
   */
  public void setPosition(long pos) throws IOException {
    this.currentPosition = pos;
    seek();
    tryAdvanceEntry();
  }

  /**
   * Advance the reader to the current position
   * @throws IOException
   */
  private void seek() throws IOException {
    if (this.currentPosition != 0) {
      this.reader.seek(this.currentPosition);
    }
  }

  /**
   * Returns the {@link Path} of the current WAL
   * @return the {@link Path} of the current WAL
   */
  public Path getCurrentPath() {
    return currentPath;
  }

  private void setCurrentPath(Path path) {
    this.currentPath = path;
  }

  private void tryAdvanceEntry() throws IOException {
    if (checkReader()) {
      readNextAndSetPosition();
      if (currentEntry == null) { // no more entries in this log file - see if log was rolled
        if (logQueue.size() > 1) { // log was rolled
          if (tryDequeueCurrentLog()) {
            openNextLog();
            readNextAndSetPosition();
          }                    
        }
        // if no other logs, we've simply hit end of current log. do nothing.
      }
    }
    // do nothing if we don't have a WAL Reader (e.g. if there's no logs in queue)
  }

  private boolean tryDequeueCurrentLog() throws IOException {
    // Before dequeueing, we should always get one more attempt at reading.
    // This is in case more entries came in after we opened the reader, 
    // and a new log was enqueued while we were reading.  See HBASE-6758
    resetReader();
    seek();
    readNextAndSetPosition();
    if (currentEntry == null) {
      logQueue.remove();
      return true;
    }
    return false;
  }
  
  private void readNextAndSetPosition() throws IOException {
    Entry nextEntry = reader.next();    
    if (filter != null) {
      // keep filtering until we get an entry, or we run out of entries to read
      while (filter.filter(nextEntry) == null && (nextEntry = reader.next()) != null);
    }        
    currentEntry = nextEntry;
    currentPosition = reader.getPosition();    
  }

  private void closeReader() throws IOException {
    if (this.reader != null) {
      this.reader.close();
      this.reader = null;
    }
  }

  // if we don't have a reader, open a reader on the next log
  private boolean checkReader() throws IOException {
    if (reader == null) {
      return openNextLog();
    }
    return true;
  }

  private boolean openNextLog() throws IOException {
    Path nextPath = logQueue.peek();
    if (nextPath != null) {
      openReader(nextPath);
      setCurrentPath(nextPath);
      return true;
    }
    return false;
  }

  private void openReader(Path path) throws IOException {
    // Detect if this is a new file, if so get a new reader else
    // reset the current reader so that we see the new data
    if (this.reader == null || !this.getCurrentPath().equals(path)) {
      closeReader();
      this.reader = WALFactory.createReader(this.fs, path, this.conf);
      setCurrentPath(path);
    } else {
      resetReader();
    }
  }

  private void resetReader() throws IOException {
    try {
      reader.reset();
    } catch (NullPointerException npe) {
      throw new IOException("NPE resetting reader, likely HDFS-4380", npe);
    }
  }

  @InterfaceAudience.Private
  public static class WALEntryStreamRuntimeException extends RuntimeException {
    private static final long serialVersionUID = -6298201811259982568L;

    public WALEntryStreamRuntimeException(IOException e) {
      super(e);
    }
  }

}
