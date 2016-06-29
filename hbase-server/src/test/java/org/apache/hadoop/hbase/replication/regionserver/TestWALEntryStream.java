package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestWALEntryStream {

  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf;
  private static FileSystem fs;
  private static MiniDFSCluster cluster;
  private static final TableName tableName = TableName.valueOf("tablename");
  private static final byte[] family = Bytes.toBytes("column");
  private static final byte[] qualifier = Bytes.toBytes("qualifier");
  private static final HRegionInfo info =
      new HRegionInfo(tableName, HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, false);
  private static final HTableDescriptor htd = new HTableDescriptor(tableName);
  private static NavigableMap<byte[], Integer> scopes;

  private WAL log;
  PriorityBlockingQueue<Path> walQueue;
  private PathWatcher pathWatcher;

  @Rule
  public TestName tn = new TestName();
  private final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(3);

    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();
    scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    walQueue = new PriorityBlockingQueue<>();
    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    pathWatcher = new PathWatcher();
    listeners.add(pathWatcher);
    final WALFactory wals = new WALFactory(conf, listeners, tn.getMethodName());
    log = wals.getWAL(info.getEncodedNameAsBytes(), info.getTable().getNamespace());
  }

  @After
  public void tearDown() throws Exception {
    log.close();
  }

  // Try out different combinations of row count and KeyValue count
  @Test
  public void testDifferentCounts() throws Exception {
    int[] NB_ROWS = { 1500, 60000 };
    int[] NB_KVS = { 1, 100 };
    // whether compression is used
    Boolean[] BOOL_VALS = { false, true };
    //long lastPosition = 0;
    for (int nbRows : NB_ROWS) {
      for (int walEditKVs : NB_KVS) {
        for (boolean isCompressionEnabled : BOOL_VALS) {          
          TEST_UTIL.getConfiguration().setBoolean(HConstants.ENABLE_WAL_COMPRESSION, isCompressionEnabled);
          mvcc.advanceTo(1);
          
          for (int i = 0; i < nbRows; i++) {
            appendToLogPlus(walEditKVs);
          }
          
          log.rollWriter();
          
          try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf)) {
            int i = 0;
            for (WAL.Entry e : entryStream) {
              assertNotNull(e);
              i++;
            }
            assertEquals(nbRows, i);
            
            // should've read all entries
            assertFalse(entryStream.hasNext());
          }
          // reset everything for next loop
          log.close();
          setUp();
        }
      }
    }
  }

  /**
   * Tests a general flow of reading appends
   */
  @Test
  public void testAppendsWithRolls() throws Exception {
    appendToLog();

    long oldPos;
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf)) {
      // There's one edit in the log, read it. Reading past it needs to throw exception
      assertTrue(entryStream.hasNext());
      WAL.Entry entry = entryStream.next();
      assertNotNull(entry);
      assertFalse(entryStream.hasNext());
      try {
        entry = entryStream.next();
        fail();
      } catch (NoSuchElementException e) {
        // expected
      }
      oldPos = entryStream.getPosition();
    }

    appendToLog();

    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf, oldPos)) {
      // Read the newly added entry, make sure we made progress
      WAL.Entry entry = entryStream.next();
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);
      oldPos = entryStream.getPosition();
    }

    // We rolled but we still should see the end of the first log and get that item
    appendToLog();
    log.rollWriter();
    appendToLog();
    
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf, oldPos)) {
      WAL.Entry entry = entryStream.next();
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);

      // next item should come from the new log
      entry = entryStream.next();
      assertNotEquals(oldPos, entryStream.getPosition());
      assertNotNull(entry);

      // no more entries to read
      assertFalse(entryStream.hasNext());
      oldPos = entryStream.getPosition();
    }
  }
  
  /**
   * Thats that if after a stream is opened, more entries come in and then the log is rolled,
   * we don't mistakenly dequeue the current log thinking we're done with it 
   */
  @Test
  public void testLogrollWhileStreaming() throws Exception {
    appendToLog(); // 1
    appendToLog();// 2
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf)) {
      appendToLog(); // 3 - our reader doesn't see this because we opened the reader before this append
      log.rollWriter(); // log roll happening while we're reading
      appendToLog(); // 4 - this append is in the rolled log
      assertNotNull(entryStream.next()); // 1
      assertNotNull(entryStream.next()); // 2
      assertEquals(2, walQueue.size()); // we should not have dequeued yet
      assertNotNull(entryStream.next()); // 3, but if implemented improperly, this would be 4 and 3 would be skipped
      assertEquals(1, walQueue.size()); //now we've dequeued and moved on to next log properly
      assertNotNull(entryStream.next()); // 4
      assertFalse(entryStream.hasNext());
    }    
  }
  
  /**
   * Tests that if writes come in while we have a stream open, we shouldn't miss them
   */
  @Test
  public void testNewEntriesWhileStreaming() throws Exception {
    long lastPosition = 0;
    appendToLog();    
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf, 0)) {
      entryStream.next(); // we've hit the end of the stream at this point
      
      // our reader doesn't see this because we opened the reader before these appends
      appendToLog(); 
      log.rollWriter();
      assertEquals(2, walQueue.size());
      appendToLog();
      
      assertFalse(entryStream.hasNext()); // we think we've hit the end of the stream...
      lastPosition = entryStream.getPosition();
    }
    // ...but that's ok as long as our next stream open picks up where we left off
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf, lastPosition)) {
      assertNotNull(entryStream.next());
      assertNotNull(entryStream.next());
      assertFalse(entryStream.hasNext()); //done
      assertEquals(1, walQueue.size());
    }    
  }
  
  @Test
  public void testEmptyStream() throws Exception {
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf, 0)) {
      
    }
  }
  
  @Test
  public void testReplicationWALEntryBatcher() throws Exception {    
    appendToLog();
    // get position after first entry
    long position;
    try (WALEntryStream entryStream = new WALEntryStream(walQueue, fs, conf)) {
      entryStream.next();
      position = entryStream.getPosition();
    }

    // start up a batcher
    ReplicationWALEntryBatcher batcher = new ReplicationWALEntryBatcher(walQueue, 0, fs, conf , getDummyFilter());
    batcher.start();
    Pair<List<Entry>, Long> entryBatch = batcher.poll(1000, TimeUnit.MILLISECONDS);
    
    // should've batched up our entry
    assertNotNull(entryBatch);
    assertEquals(1, entryBatch.getFirst().size());
    assertEquals(position, entryBatch.getSecond().longValue());
    
    batcher.setWorkerRunning(false);
    appendToLog();
    appendToLog();
    appendToLog();
    batcher.setWorkerRunning(true);
    
    entryBatch = batcher.poll(1000, TimeUnit.MILLISECONDS);
    assertEquals(3, entryBatch.getFirst().size());
    batcher.setWorkerRunning(false);
  }

  private void appendToLog() throws IOException {
    appendToLogPlus(1);
  }

  private void appendToLogPlus(int count) throws IOException {
    final long txid = log.append(info,
      new WALKey(info.getEncodedNameAsBytes(), tableName, System.currentTimeMillis(), mvcc, scopes),
      getWALEdits(count), true);
    log.sync(txid);
  }

  private WALEdit getWALEdits(int count) {
    WALEdit edit = new WALEdit();
    for (int i = 0; i < count; i++) {
      edit.add(new KeyValue(Bytes.toBytes(System.currentTimeMillis()), family, qualifier,
          System.currentTimeMillis(), qualifier));
    }
    return edit;
  }
  
  private WALEntryFilter getDummyFilter() {
    return new WALEntryFilter() {
      
      @Override
      public Entry filter(Entry entry) {
        return entry;
      }
    };
  }

  class PathWatcher extends WALActionsListener.Base {

    Path currentPath;

    @Override
    public void preLogRoll(Path oldPath, Path newPath) throws IOException {
      walQueue.add(newPath);
      currentPath = newPath;
    }
  }

}
