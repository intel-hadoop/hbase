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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.SortedSet;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.KeyValueHeap.KVScannerComparator;

/**
 * ROLLING_SCAN is used to optimize the performance of small scan when the key is mono-incremental
 * across different store files. When setting ROLLING_SCAN as true, we will open scanner for store
 * files in a rolling manner. At first, we will only create scanner for the store file which is
 * immediate after the start Key. When we scan to a key which may exists in another store file, then
 * we open a new scanner for the store files.
 *
 * That means we will delaying creating of a new File scanner to the point when the actually read
 * happens on the file.
 */
public class RollingStoreFileScanner implements KeyValueScanner, ChangedReadersObserver {

  static final Log LOG = LogFactory.getLog(Store.class);
  private boolean seekDone = false;
  private RollingHeap heap;
  private boolean closing;
  private Store store;
  private FirstKeySortedStoreFiles firstKeySortedStoreFiles;
  private KVComparator kvComparator;
  private boolean updated;
  private Scan scan;

  public RollingStoreFileScanner(Store store, FirstKeySortedStoreFiles firstKeySortedStoreFiles,
      final NavigableSet<byte[]> targetCols, Scan scan) throws IOException {
    this.store = store;
    this.scan = scan;
    this.firstKeySortedStoreFiles = firstKeySortedStoreFiles;
    Collection<StoreFile> storeFiles = firstKeySortedStoreFiles.getStoreFiles(scan);
    this.heap = new RollingHeap(storeFiles.iterator(), new KVComparator());

    this.kvComparator = new KVComparator();
    store.addChangedReaderObserver(this);
  }

  @Override
  public synchronized Cell peek() {
    return heap.peek();
  }

  @Override
  public synchronized Cell next() throws IOException {
    checkUpdate();
    return heap.poll();
  }

  @Override
  public synchronized boolean reseek(Cell key) throws IOException {
    return seek(key);
  }

  @Override
  public synchronized void close() {
    if (this.closing)
      return;
    this.closing = true;
    this.heap.close();
    if (this.store != null) {
      this.store.deleteChangedReaderObserver(this);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized boolean seek(Cell key) throws IOException {
    checkUpdate();
    seekDone = true;
    return heap.seek(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized boolean requestSeek(Cell kv, boolean forward, boolean useBloom)
      throws IOException {
    return seek(kv);
  }

  @Override
  public boolean realSeekDone() {
    return seekDone;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void enforceSeek() throws IOException {
    return;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isFileScanner() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns, long oldestUnexpiredTS) {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void updateReaders() throws IOException {
    if (this.closing) {
      return;
    }
    this.updated = true;
  }

  /**
   * {@inheritDoc}
   */
  private void checkUpdate() throws IOException {
    if (updated) {
      updated = false;

      Collection<StoreFile> newStoreFiles = null;

      seekDone = false;
      Cell lastTop = this.heap.peek();
      if (null == lastTop) {
        newStoreFiles = firstKeySortedStoreFiles.getStoreFiles(scan);
      } else {
        Scan newScan = new Scan(scan);
        newScan.setStartRow(lastTop.getRow());
        newStoreFiles = firstKeySortedStoreFiles.getStoreFiles(newScan);
      }
      heap.close();
      this.heap = new RollingHeap(newStoreFiles.iterator(), kvComparator);
      if (null != lastTop) {
        heap.seek(lastTop);
      }
      seekDone = true;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean seekToPreviousRow(Cell key) throws IOException {
    throw new NotImplementedException("Not implemented");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean seekToLastRow() throws IOException {
    throw new NotImplementedException("Not implemented");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    throw new NotImplementedException("Not implemented");
  }

  public static class RollingHeap {

    private PriorityQueue<KeyValueScanner> heap = null;
    private final int initialHeapSize = 1;

    private RollingBar bar = null;

    private Iterator<StoreFile> files;

    private KVComparator kvComparator;

    private boolean closed;

    public RollingHeap(Iterator<StoreFile> storeFiles, KVComparator comparator) throws IOException {
      KVScannerComparator scannerComparator = new KVScannerComparator(comparator);
      this.kvComparator = comparator;

      this.files = storeFiles;

      this.bar = RollingBar.MIN;

      heap = new PriorityQueue<KeyValueScanner>(initialHeapSize, scannerComparator);
    }

    /**
     * retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
     */
    public Cell peek() {
      if (closed) {
        return null;
      }
      return peek(heap);
    }

    /**
     * retrieves and removes the head of this queue, or returns null if this queue is empty.
     */
    public Cell poll() throws IOException {
      if (closed) {
        return null;
      }
      KeyValueScanner scanner = heap.poll();
      if (null == scanner) {
        return null;
      }
      Cell current = scanner.next();
      if (null == scanner.peek()) {
        scanner.close();
      } else {
        heap.add(scanner);
      }
      if (needRolling(heap, bar)) {
        seek(bar.get());
      }
      return current;
    }

    private int compare(Cell kv1, Cell kv2) {
      return kvComparator.compare(kv1, kv2);
    }

    public void close() {
      if (!closed) {
        KeyValueScanner scanner;
        while ((scanner = this.heap.poll()) != null) {
          scanner.close();
        }
        closed = true;
      }
    }

    public boolean seek(Cell seek) throws IOException {
      if (closed) {
        return false;
      }
      seek(heap, seek);

      if (!needRolling(heap, bar)) {
        return true;
      }

      while (files.hasNext()) {
        StoreFile file = files.next();
        byte[] firstKey = getFirstKey(file);
        if (null == firstKey) {
          continue;
        }
        bar.set(firstKey);

        if (!needRolling(heap, bar)) {
          break;
        }

        byte[] lastKey = getLastKey(file);
        if (null == lastKey || compareKey(seek, lastKey) > 0) {
          continue;
        }

        StoreFileScanner scanner = getScanner(file);
        boolean found = scanner.seek(seek);
        if (!found) {
          scanner.close();
        } else {
          heap.add(scanner);
        }
      }

      if (!files.hasNext()) {
        bar = RollingBar.MAX;
      }
      return null != heap.peek();
    }

//    private int compareKey(KeyValue kv1, byte[] k2) {
//      int ret = kvComparator.compare(kv1.getBuffer(), kv1.getOffset() + KeyValue.ROW_OFFSET,
//          kv1.getKeyLength(), k2, 0, k2.length);
//      return ret;
//    }
    private int compareKey(Cell kv1, byte[] k2) {
      int ret = kvComparator.compare(kv1.getRowArray(), kv1.getRowOffset() + KeyValue.ROW_OFFSET,
          kv1.getRowLength(), k2, 0, k2.length);
      return ret;
    }

    private boolean needRolling(PriorityQueue<KeyValueScanner> heap, RollingBar bar) {
      if (null == heap) {
        return true;
      }
      Cell heapTop = peek(heap);
      if (null != heapTop && bar.compareTo(heapTop) > 0) {
        return false;
      }
      return true;
    }

    private byte[] getFirstKey(StoreFile file) {
      if (null == file) {
        return null;
      }
      return file.getReader().getFirstKey();
    }

    private byte[] getLastKey(StoreFile file) {
      if (null == file) {
        return null;
      }
      StoreFile.Reader fileReader = file.getReader();
      if (null == fileReader) {
        return null;
      }
      if (null == fileReader.getLastKey()) {
        return null;
      }
      return fileReader.getLastKey();
    }

    private StoreFileScanner getScanner(StoreFile file) {
      StoreFile.Reader reader = file.getReader();
      StoreFileScanner scanner = reader.getStoreFileScanner(false, true);
      return scanner;
    }

    /**
     * Gets the head of the RollingHeap.
     * @param heap
     * @return the head of the RollingHeap.
     */
    private Cell peek(PriorityQueue<KeyValueScanner> heap) {
      if (null == heap) {
        return null;
      }
      if (null == heap.peek()) {
        return null;
      }
      return heap.peek().peek();
    }

    private boolean seek(PriorityQueue<KeyValueScanner> heap, Cell key) throws IOException {

      if (null == heap || null == heap.peek()) {
        return false;
      }

      Cell top = heap.peek().peek();
      if (compare(key, top) <= 0) {
        return true;
      }

      KeyValueScanner scanner = null;
      while (null != (scanner = heap.poll())) {
        Cell current = scanner.peek();
        if (null == current) {
          scanner.close();
          continue;
        }

        if (compare(key, current) <= 0) {
          heap.add(scanner);
          return true;
        } else {
          boolean found = scanner.seek(key);
          if (!found) {
            scanner.close();
          } else {
            heap.add(scanner);
          }
        }
      }
      return false;
    }
  }

  private static class RollingBar {
    byte[] key;
    Cell kv;

    private final static KeyValue MIN_KEYVALUE;
    private final static KeyValue MAX_KEYVALUE;
    static {
      byte[] min = new byte[0];
      MIN_KEYVALUE = KeyValueUtil.createFirstOnRow(min);

      int limit = 1024; // max key in 1024 space
      byte[] max = new byte[limit];
      Arrays.fill(max, (byte) 0xff);
      MAX_KEYVALUE = KeyValueUtil.createFirstOnRow(max);
    }

    public static RollingBar MIN = new RollingBar(MIN_KEYVALUE);
    public static RollingBar MAX = new RollingBar(MAX_KEYVALUE);

    public RollingBar(Cell kv) {
      this.kv = kv;
    }

    public void set(byte[] key) {
      this.key = key;
      kv = null;
    }

    public Cell get() {
      if (null == kv) {
        kv = KeyValue.createKeyValueFromKey(key);
      }
      return kv;
    }

    public void set(Cell kv) {
      this.kv = kv;
      key = null;
    }

    public int compareTo(Cell kv) {
      if (this == MIN) {
        return -1;
      }
      if (this == MAX) {
        return 1;
      }
      return KeyValue.COMPARATOR.compare(get(), kv);
    }
  }

  /**
   * Since it only compares with the MemStoreScanner, the method directly returns 0.
   * @return the sequence id of this scanner.
   */
  @Override
  public long getSequenceID() {
    return 0;
  }
}
