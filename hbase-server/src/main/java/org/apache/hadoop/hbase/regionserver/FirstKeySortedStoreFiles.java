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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.collections.collection.CompositeCollection;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;

/**
 * FirstKeySortedStoreFiles will watch on the store file change and re-sorted
 * it based on custom rules.
 *
 */
public class FirstKeySortedStoreFiles implements ChangedReadersObserver,
    Closeable {
  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private Comparator<StoreFile> comparator;
  private Store store;
  private List<StoreFile> storeFiles;
  private boolean closing;
  private IntervalTree intervalTree;

  public FirstKeySortedStoreFiles(Store store) {
    this.comparator = new FirstKeyComparator();
    this.store = store;

    storeFiles = sortAndClone(store.getStorefiles());
    this.intervalTree = new IntervalTree(storeFiles);
    store.addChangedReaderObserver(this);
  }

  /**
   * Get the StoreFiles
   * @param scan
   * @return the collection of StoreFile
   */
  @SuppressWarnings("unchecked")
  public Collection<StoreFile> getStoreFiles(Scan scan) {
    lock.readLock().lock();
    IntervalTree intervalTree = this.intervalTree;
    List<StoreFile> storeFiles = this.storeFiles;
    lock.readLock().unlock();

    if (null == scan || scan.getStartRow().length == 0) {
      return storeFiles;
    }
    CompositeCollection collections = new CompositeCollection();

    byte[] search = KeyValueUtil.createFirstOnRow(scan.getStartRow()).getKey();

    List<StoreFile> intsected = intervalTree.findIntesection(search);
    if (null != intsected && intsected.size() != 0) {
      collections.addComposited(intsected);
    }
    int index = binarySearch(storeFiles, search);
    if (-1 != index) {
      List<StoreFile> subList = storeFiles.subList(index, storeFiles.size());
      collections.addComposited(subList);
    }
    return collections;
  }

  /**
   * binay search
   * @param storeFiles
   * @param search
   * @return position of the key
   */
  private int binarySearch(List<StoreFile> storeFiles, byte[] search) {
    if (null == storeFiles || storeFiles.size() == 0) {
      return -1;
    }
    int low = 0;
    int high = storeFiles.size()-1;
    int result = -1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      StoreFile midVal = storeFiles.get(mid);
      int cmp = KeyValue.COMPARATOR.compareFlatKey(midVal.getReader().getFirstKey(), search);
      if (cmp < 0) {
        low = mid + 1;
      }
      else if (cmp > 0) {
        high = mid - 1;
      }
      else {
        result = mid + 1; // key found
        break;
      }
    }
    if (result == -1) {
      result = low + 1;
    }
    return (result >= storeFiles.size()) ? -1 : result;
  }

  /**
   * sort the StoreFiles
   * @param input
   * @return the sorted StoreFiles
   */
  private List<StoreFile> sortAndClone(Collection<StoreFile> input) {
    List<StoreFile> storeFiles = new ArrayList<StoreFile>();
    for (StoreFile f : input) {
      if (null == f.getReader()) {
        continue;
      }
      byte[] firstKey = f.getReader().getFirstKey();
      if (null == firstKey || firstKey.length == 0) {
        continue;
      }
      storeFiles.add(f);
    }
    Collections.sort(storeFiles, comparator);
    return storeFiles;
  }

  @Override
  public void updateReaders() throws IOException {
    lock.writeLock().lock();
    try {
      this.storeFiles = sortAndClone(store.getStorefiles());
      this.intervalTree = new IntervalTree(storeFiles);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    lock.writeLock().lock();
    try {
      if (!closing) {
        closing = true;
        if (null != store) {
          store.deleteChangedReaderObserver(this);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * The comparator which order the StoreFiles by their first keys.
   */
  public static class FirstKeyComparator implements Comparator<StoreFile> {

    private KVComparator keyComparator = KeyValue.COMPARATOR;

    @Override
    public int compare(StoreFile o1, StoreFile o2) {

      StoreFile.Reader o1Reader = o1.getReader();
      if (null == o1Reader) {
        return -1;
      }
      byte[]  k1 = o1.getReader().getFirstKey();
      if (null == k1) {
        return -1;
      }
      StoreFile.Reader o2Reader = o2.getReader();
      if (null == o2Reader) {
        return -1;
      }
      byte[]  k2 = o2.getReader().getFirstKey();
      if (null == k2) {
        return -1;
      }
      return keyComparator.compareFlatKey(k1, k2);
    }
  }


  /**
   * The tree node.
   */
  private static class TreeNode {

    public TreeNode(StoreFile storeFile) {
      this.file = storeFile;
      this.maxKey = file.getReader().getLastKey();
    }

    StoreFile file;
    byte[] maxKey;
  }

  /**
   * The interval tree.
   */
  private class IntervalTree {

    private List<TreeNode> nodes;

    IntervalTree(List<StoreFile> files) {
      buildTree(files);
    }

    /**
     * Builds a interval tree.
     *
     * @param files
     */
    private void buildTree(List<StoreFile> files){
      int depth = getDepth(files.size());
      int length = 1 << depth;
      nodes = new ArrayList<TreeNode>(length);
      for (int i = 0; i < length; i++)  {
        nodes.add(null);
      }
      buildTree(files, 0, files.size(), 0, nodes);
      updateMaxBoundary(nodes);
    }


  /**
   * Update the max boundary in the tree.
   *
   * @param nodes
   */
    private void updateMaxBoundary(List<TreeNode> nodes) {
      for (int i = nodes.size() - 1; i >=0; i--) {
        TreeNode node = nodes.get(i);
        if (null == node) {
          continue;
        }
        int parentIndex = (i - 1)/2;
        if (parentIndex <= 0) {
          continue;
        }
        TreeNode parent = nodes.get(parentIndex);
        parent.maxKey = max(parent.maxKey, node.maxKey);
      }
    }

    public List<StoreFile> findIntesection(byte[] key) {
      if (null == key || key.length == 0) {
        return null;
      }
      List<StoreFile> results = new ArrayList<StoreFile>();
      findIntesection(key, results, 0);
      Collections.sort(results, comparator);
      return results;
    }

    private void findIntesection(byte[] key, List<StoreFile> result, int index) {
      if (index >= nodes.size()) {
        return;
      }
      TreeNode node = nodes.get(index);
      if (null == node) {
        return;
      }
      if (compareKey(key, node.maxKey) > 0) {
        return;
      }
      if (compareKey(key, node.file.getReader().getFirstKey()) < 0){
        findIntesection(key, result, 2 * index + 1);
        return;
      }
      if (compareKey(key, node.file.getReader().getLastKey()) <= 0){
        result.add(node.file);
      }

      findIntesection(key, result, 2 * index + 1);
      findIntesection(key, result, 2 * index + 2);
    }

    /**
     * Compare two keys.
     *
     * @param key1
     * @param key2
     */
    private int compareKey(byte[] key1, byte[] key2) {
      return KeyValue.COMPARATOR.compareFlatKey(key1, key2);
    }

    /**
     * Get the max key.
     *
     * @param key1
     * @param key2
     */
    private byte[] max(byte[] key1, byte[] key2) {
      if (null == key1) {
        return key2;
      }
      if (null == key2) {
        return key1;
      }
      return (KeyValue.COMPARATOR.compareFlatKey(key1, key2) >= 0) ? key1 : key2;
    }

   /**
     * Get the depth of the true through the number of storefile.
     *
     * @param size
     */
    private int getDepth(int size) {
      int depth = 0;
      while (0 != size) {
        depth++;
        size = size >> 1;
      }
      return depth;
    }


    /**
     * Build the tree.
     *
     * @param data
     * @param start
     * @param end
     * @param position
     * @param result
     */
    private void buildTree(List<StoreFile> data, int start, int end,
        int position, List<TreeNode> result)
        {
      if (end <= start) {
        return;
      }
      int middle = (start + end) /2;
      result.set(position, new TreeNode(data.get(middle)));
      int leftChildPosition = 2 * position + 1;
      int rightChildPosition = 2*position + 2;

      buildTree(data, start, middle, leftChildPosition, result);
      buildTree(data, middle + 1, end, rightChildPosition, result);
    }
  }
}
