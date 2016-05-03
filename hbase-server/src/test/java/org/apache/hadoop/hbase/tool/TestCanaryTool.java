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

package org.apache.hadoop.hbase.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.Matchers.eq;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;

@RunWith(MockitoJUnitRunner.class)
@Category({MediumTests.class})
public class TestCanaryTool {

  private HBaseTestingUtility testingUtility;
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] COLUMN = Bytes.toBytes("col");

  @Before
  public void setUp() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
    LogManager.getRootLogger().addAppender(mockAppender);
  }

  @After
  public void tearDown() throws Exception {
    testingUtility.shutdownMiniCluster();
    LogManager.getRootLogger().removeAppender(mockAppender);
  }

  @Mock
  Appender mockAppender;

  @Test
  public void testBasicCanaryWorks() throws Exception {
    TableName tableName = TableName.valueOf("testTable");
    HTable table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i=0; i<1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.addColumn(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    Canary.RegionServerStdOutSink sink = spy(new Canary.RegionServerStdOutSink());
    Canary canary = new Canary(executor, sink);
    String[] args = { "-t", "10000", "testTable" };
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);
    verify(sink, atLeastOnce())
        .publishReadTiming(isA(HRegionInfo.class), isA(HColumnDescriptor.class), anyLong());
  }

  //no table created, so there should be no regions
  @Test
  public void testRegionserverNoRegions() throws Exception {
    runRegionserverCanary();
    verify(mockAppender).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Regionserver not serving any regions");
      }
    }));
  }

  //by creating a table, there shouldn't be any region servers not serving any regions
  @Test
  public void testRegionserverWithRegions() throws Exception {
    TableName tableName = TableName.valueOf("testTable");
    testingUtility.createTable(tableName, new byte[][] { FAMILY });
    runRegionserverCanary();
    verify(mockAppender, never()).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Regionserver not serving any regions");
      }
    }));
  }

  //no table created, so no regions, but regionserver is marked as draining
  @Test
  public void testDrainingRegionServer() throws Exception {
    //mark the regionserver as draining in ZK
    ServerName rs = testingUtility.getHBaseCluster().getRegionServer(0).getServerName();
    Configuration conf = testingUtility.getConfiguration();
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
      HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String drainingZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.draining.rs", "draining"));
    String rsZNode = ZKUtil.joinZNode(drainingZNode, rs.getServerName());
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "draining_servers", null);
    ZKUtil.createAndFailSilent(zkw, rsZNode);

    //make sure master watcher was triggered and updated its list of draining servers
    Thread.sleep(500);
    HMaster master = testingUtility.getHBaseCluster().getMaster();
    List<ServerName> drainingServersList = master.getServerManager().getDrainingServersList();
    assertEquals(1, drainingServersList.size());
    assertEquals(rs, drainingServersList.get(0));

    //canary shouldn't log anything since regionserver is draining
    runRegionserverCanary();
    verify(mockAppender, never()).doAppend(argThat(new ArgumentMatcher<LoggingEvent>() {
      @Override
      public boolean matches(Object argument) {
        return ((LoggingEvent) argument).getRenderedMessage().contains("Regionserver not serving any regions");
      }
    }));
  }

  private void runRegionserverCanary() throws Exception {
    ExecutorService executor = new ScheduledThreadPoolExecutor(1);
    Canary canary = new Canary(executor, new Canary.RegionServerStdOutSink());
    String[] args = { "-t", "10000", "-regionserver"};
    ToolRunner.run(testingUtility.getConfiguration(), canary, args);
  }

}

