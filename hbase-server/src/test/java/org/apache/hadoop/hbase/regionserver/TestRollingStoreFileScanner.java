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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.util.Progressable;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test class for the Rolling scan store file
 */
@Category(MediumTests.class)
public class TestRollingStoreFileScanner extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestStore.class);

  HStore store;
  byte [] table = Bytes.toBytes("table");
  byte [] family = Bytes.toBytes("family");

  byte [] row = Bytes.toBytes("row");
  byte [] qf1 = Bytes.toBytes("qf1");
  byte [] qf3 = Bytes.toBytes("qf3");
  byte [] qf5 = Bytes.toBytes("qf5");

  NavigableSet<byte[]> qualifiers =
    new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);

  List<KeyValue> expected = new ArrayList<KeyValue>();
  List<KeyValue> result = new ArrayList<KeyValue>();

  long id = System.currentTimeMillis();
  Get get = new Get(row);

  private HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getDataTestDir("TestStore").toString();


  /**
   * Setup
   * @throws IOException
   */
  @Override
  public void setUp() throws IOException {
    qualifiers.add(qf1);
    qualifiers.add(qf3);
    qualifiers.add(qf5);

    Iterator<byte[]> iter = qualifiers.iterator();
    while(iter.hasNext()){
      byte [] next = iter.next();
      expected.add(new KeyValue(row, family, next, 1, (byte[])null));
      get.addColumn(family, next);
    }
  }

  private void init(String methodName) throws IOException {
    init(methodName, HBaseConfiguration.create());
  }

  private void init(String methodName, Configuration conf)
  throws IOException {
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    // some of the tests write 4 versions and then flush
    // (with HBASE-4241, lower versions are collected on flush)
    hcd.setMaxVersions(4);
    init(methodName, conf, hcd);
  }

  private void init(String methodName, Configuration conf,
      HColumnDescriptor hcd) throws IOException {
    //Setting up a Store
    Path basedir = new Path(DIR+methodName);
    Path logdir = new Path(DIR+methodName+"/logs");
    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("table"));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);

    store = new HStore(region, hcd, conf);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  private static void flushStore(HStore store, long id) throws IOException {
    StoreFlushContext storeFlushCtx = store.createFlushContext(id);
    storeFlushCtx.prepare();
    storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
    storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));
  }

  /**
   * Generate a list of KeyValues for testing based on given parameters.
   * @param timestamps
   * @param numRows
   * @param qualifier
   * @param family
   * @return the KeyValue created.
   */
  List<KeyValue> getKeyValueSet(long[] timestamps, int numRows,
      byte[] qualifier, byte[] family, int start) {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
    for (int i=0;i < numRows;i++) {
      byte[] b = Bytes.toBytes(i + start);
      for (long timestamp: timestamps) {
        kvList.add(new KeyValue(b, family, qualifier, timestamp, b));
      }
    }
    return kvList;
  }

  /**
   * test the rolling scan with no overlap.
   */
  public void testRollingScanNoOverlap() throws IOException {
    int numRows = 100;
    long[] timestamps1 = new long[] {1};

    init(this.getName());

    List<KeyValue> kvList1 = getKeyValueSet(timestamps1,numRows, qf1, family, 0);
    for (KeyValue kv : kvList1) {
      this.store.add(kv);
    }

    this.store.snapshot();
    flushStore(store, id++);

    int newOffset = 10000;
    List<KeyValue> kvList2 = getKeyValueSet(timestamps1,numRows, qf1, family, newOffset);
    for(KeyValue kv : kvList2) {
      this.store.add(kv);
    }

    this.store.snapshot();
    flushStore(store, id++);
    FirstKeySortedStoreFiles firstKeySortedStoreFiles = new FirstKeySortedStoreFiles(store);

    RollingStoreFileScanner scanner = new RollingStoreFileScanner(store, firstKeySortedStoreFiles
        , null, new Scan());
    scanner.seek(KeyValueUtil.createFirstOnRow(new byte[] {}));
    Cell kv = null;
    int count = 0;
    while(null != (kv = scanner.next())) {
      count++;
    }
    assertEquals(count, numRows * 2);
    scanner.close();
  }

  /**
   * test the rolling scan with partial overlap.
   */
  public void testRollingScanOverlap() throws IOException {
    int numRows = 100;
    long[] timestamps1 = new long[] {1};

    init(this.getName());

    List<KeyValue> kvList1 = getKeyValueSet(timestamps1,numRows, qf1, family, 0);
    for (KeyValue kv : kvList1) {
      this.store.add(kv);
    }

    this.store.snapshot();
    flushStore(store, id++);

    List<KeyValue> kvList2 = getKeyValueSet(timestamps1,numRows, qf1, family, 0);
    for(KeyValue kv : kvList2) {
      this.store.add(kv);
    }

    this.store.snapshot();
    flushStore(store, id++);

    FirstKeySortedStoreFiles firstKeySortedStoreFiles = new FirstKeySortedStoreFiles(store);

    RollingStoreFileScanner scanner = new RollingStoreFileScanner(store, firstKeySortedStoreFiles
        , null, new Scan());

    scanner.seek(KeyValueUtil.createFirstOnRow(new byte[] {}));
    int position = 0;
    Cell kv = null;
    while(null != (kv = scanner.next())) {
      byte[] row = kv.getRow();
      int rowId = Bytes.toInt(row);
      assertEquals(rowId, (position++)/2);
    }
    scanner.close();
  }

  /**
   * test the rolling scan with no overlap in end to end.
   */
  public void testEndToEndRollingScanNoOverlap() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.region.classes",
        RollingScanRegionObserver.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      byte[] tableName = Bytes.toBytes("RollingScanTable");
      HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
      HTable table = createTable(admin, tableName);
      int numRows = 100;
      long[] timestamps1 = new long[] { 1 };
      List<KeyValue> kvList1 = getKeyValueSet(timestamps1, numRows, qf1, family, 0);
      List<Put> puts = new ArrayList<Put>();
      for (KeyValue kv : kvList1) {
        Put put = new Put(kv.getRow());
        put.add(kv);
        puts.add(put);
      }
      table.put(puts);
      table.flushCommits();
      admin.flush(tableName);

      puts.clear();
      kvList1.clear();
      kvList1 = getKeyValueSet(timestamps1, numRows, qf1, family, 100);
      for (KeyValue kv : kvList1) {
        Put put = new Put(kv.getRow());
        put.add(kv);
        puts.add(put);
      }
      table.put(puts);
      table.flushCommits();
      admin.flush(tableName);

      // in the memstore.
      puts.clear();
      kvList1.clear();
      kvList1 = getKeyValueSet(timestamps1, numRows, qf1, family, 200);
      for (KeyValue kv : kvList1) {
        Put put = new Put(kv.getRow());
        put.add(kv);
        puts.add(put);
      }
      table.put(puts);
      table.flushCommits();
      ResultScanner rs = table.getScanner(new Scan());
      int count = 0;
      boolean stop = false;
      while (!stop) {
        Result r = rs.next();
        if (r != null) {
          count++;
        } else {
          stop = true;
          break;
        }
      }
      assertEquals(300, count);
      table.close();
      admin.close();
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  /**
   * test the rolling scan with partial overlap in end to end.
   */
  public void testEndToEndRollingScanPartialOverlap() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.coprocessor.region.classes",
        RollingScanRegionObserver.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      byte[] tableName = Bytes.toBytes("RollingScanTable");
      HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
      HTable table = createTable(admin, tableName);
      int numRows = 100;
      long[] timestamps1 = new long[] { 1 };
      List<KeyValue> kvList1 = getKeyValueSet(timestamps1, numRows, qf1, family, 0);
      List<Put> puts = new ArrayList<Put>();
      for (KeyValue kv : kvList1) {
        Put put = new Put(kv.getRow());
        put.add(kv);
        puts.add(put);
      }
      table.put(puts);
      table.flushCommits();
      admin.flush(tableName);

      puts.clear();
      kvList1.clear();
      kvList1 = getKeyValueSet(timestamps1, numRows, qf1, family, 50);
      for (KeyValue kv : kvList1) {
        Put put = new Put(kv.getRow());
        put.add(kv);
        puts.add(put);
      }
      table.put(puts);
      table.flushCommits();
      admin.flush(tableName);

      // in the memstore.
      puts.clear();
      kvList1.clear();
      kvList1 = getKeyValueSet(timestamps1, numRows, qf1, family, 100);
      for (KeyValue kv : kvList1) {
        Put put = new Put(kv.getRow());
        put.add(kv);
        puts.add(put);
      }
      table.put(puts);
      table.flushCommits();
      ResultScanner rs = table.getScanner(new Scan());
      int count = 0;
      boolean stop = false;
      while (!stop) {
        Result r = rs.next();
        if (r != null) {
          count++;
        } else {
          stop = true;
          break;
        }
      }
      assertEquals(200, count);
      table.close();
      admin.close();
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  /**
   * create a HTable based on given parameters.
   *
   * @param admin
   * @param tableName
   * @return the HTable created.
   */
  private HTable createTable(HBaseAdmin admin, byte[] tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.setValue(Bytes.toBytes(RollingScanConstants.ROLLING_SCAN), Bytes.toBytes(true));
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    htd.addFamily(hcd);
    admin.createTable(htd);
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    return ht;
  }

}

