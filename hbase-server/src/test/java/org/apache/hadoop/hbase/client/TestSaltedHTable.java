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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class TestSaltedHTable {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] TEST_TABLE = Bytes.toBytes("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");

  private static final byte[] qualifierCol1 = Bytes.toBytes("col1");

  private static final byte[] bytes1 = Bytes.toBytes(1);
  private static final byte[] bytes2 = Bytes.toBytes(2);
  private static final byte[] bytes3 = Bytes.toBytes(3);
  private static final byte[] bytes4 = Bytes.toBytes(4);
  private static final byte[] bytes5 = Bytes.toBytes(5);
  private static final byte[] bytes6 = Bytes.toBytes(6);
  private static final byte[] bytes7 = Bytes.toBytes(7);

  private HTable table;
  private SaltedHTable saltedHTable;
  private List<Put> puts = new ArrayList<Put>();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    table = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    table.setAutoFlush(false);

    KeySalter salter = new OneBytePrefixKeySalter();
    saltedHTable = new SaltedHTable(table, salter);

    Put puta = new Put(ROW_A);
    puta.add(TEST_FAMILY, qualifierCol1, bytes1);
    puts.add(puta);

    Put putb = new Put(ROW_B);
    putb.add(TEST_FAMILY, qualifierCol1, bytes2);
    puts.add(putb);

    Put putc = new Put(ROW_C);
    putc.add(TEST_FAMILY, qualifierCol1, bytes3);
    puts.add(putc);

    saltedHTable.put(puts);
    saltedHTable.flushCommits();
  }

  @After
  public void after() throws Exception {
    try {
      if (table != null) {
        table.close();
      }
      if (saltedHTable != null) {
        saltedHTable.close();
      }
    } finally {
      TEST_UTIL.deleteTable(TEST_TABLE);
    }
  }

  /**
   *test the methods of put,delete and scan.
   * @throws Exception
   */
  @Test
  public void TestPutDeleteScan()  throws Exception {
    Scan scan = new Scan();
    int count = 0;
    ResultScanner scanner = saltedHTable.getScanner(scan);
    Result result = null;
    while(null != (result = scanner.next()) ) {
      count++;
    }
    assertEquals(count, puts.size());

    count = 0;
    List<Delete> deletes = new ArrayList<Delete>();
    deletes.add( new Delete(ROW_A) );
    deletes.add( new Delete(ROW_B) );
    deletes.add( new Delete(ROW_C) );

    saltedHTable.delete(deletes);

    scan = new Scan();
    scan.addColumn(TEST_FAMILY, qualifierCol1);
    count = 0;
    scanner = saltedHTable.getScanner(scan);
    result = null;
    while(null != (result = scanner.next()) ) {
      count++;
    }
    assertEquals(count, 0);
  }

  @Test
  public void TestCheckAndPut() throws IOException {
    Put putA = new Put(ROW_A).add(TEST_FAMILY, qualifierCol1, bytes5);
    assertFalse(saltedHTable.checkAndPut(ROW_A, TEST_FAMILY, qualifierCol1, /* expect */bytes2,
        putA/* newValue */));
    assertTrue(saltedHTable.checkAndPut(ROW_A, TEST_FAMILY, qualifierCol1, /* expect */bytes1,
        putA/* newValue */));
    checkRowValue(ROW_A, bytes5);

    Put putB = new Put(ROW_B).add(TEST_FAMILY, qualifierCol1, bytes6);
    assertFalse(saltedHTable.checkAndPut(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes3, putB/* newValue */));
    assertTrue(saltedHTable.checkAndPut(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes2, putB/* newValue */));
    checkRowValue(ROW_B, bytes6);

    Put putC = new Put(ROW_C).add(TEST_FAMILY, qualifierCol1, bytes7);
    assertFalse(saltedHTable.checkAndPut(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.LESS,
        bytes4, putC/* newValue */));
    assertTrue(saltedHTable.checkAndPut(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.LESS,
        bytes2, putC/* newValue */));
    checkRowValue(ROW_C, bytes7);
  }

  @Test
  public void TestCheckAndDelete() throws IOException {
    Delete deleteA = new Delete(ROW_A);
    assertFalse(saltedHTable.checkAndDelete(ROW_A, TEST_FAMILY, qualifierCol1, bytes2, deleteA));
    assertTrue(saltedHTable.checkAndDelete(ROW_A, TEST_FAMILY, qualifierCol1, bytes1, deleteA));
    checkRowValue(ROW_A, null);

    Delete deleteB = new Delete(ROW_B);
    assertFalse(saltedHTable.checkAndDelete(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes3, deleteB));
    assertTrue(saltedHTable.checkAndDelete(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes2, deleteB));
    checkRowValue(ROW_B, null);

    Delete deleteC = new Delete(ROW_C);
    assertFalse(saltedHTable.checkAndDelete(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.GREATER,
        bytes2, deleteC));
    assertTrue(saltedHTable.checkAndDelete(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.GREATER,
        bytes4, deleteC));
    checkRowValue(ROW_C, null);
  }

    private void checkRowValue(byte[] row, byte[] expectedValue) throws IOException {
    Get get = new Get(row).addColumn(TEST_FAMILY, qualifierCol1);
    Result result = saltedHTable.get(get);
    byte[] actualValue = result.getValue(TEST_FAMILY, qualifierCol1);
    assertArrayEquals(expectedValue, actualValue);
  }
}

