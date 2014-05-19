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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.util.MergeSortIterator;

/**
 * MergeSortScanner will merge the result of different scan
 * We assume the result of each scan is already sorted incrementally.
 *
 * Suppose a table row key definition: id + timestamp
 * Suppose two scan, scan1: id = 1, scan2: id = 2
 * We want the result of the two scan's ordered by timestamp
 * We can use this MergeSortScanner.
 *
 */
public class MergeSortScanner implements ResultScanner {

  private List<ScannerIterator> iters;
  private MergeSortIterator<Result> iter;
  private boolean closed = false;

  public MergeSortScanner(Scan[] scans, HTableInterface table, int commonPrefixLength)
      throws IOException {

    this.iters = new ArrayList<ScannerIterator>();

    for (int i = 0; i < scans.length; i++) {
      ResultScanner scanner = table.getScanner(scans[i]);
      iters.add(new ScannerIterator(scanner));
    }
    this.iter = new MergeSortIterator<Result>(iters,
        new IgnorePrefixComparator(commonPrefixLength));
  }

  @Override
  public Result next() throws IOException {
    if (this.closed)
      return null;

    try {
      return iter.next();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public Result[] next(int nbRows) throws IOException {
    ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
    for(int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }

  @Override
  public void close() {
    if (closed)
      return;

    for (ScannerIterator it : iters) {
      if (!it.isClosed()) {
        it.close();
      }
    }
    closed = true;
  }

  @Override
  public Iterator<Result> iterator() {
    return iter;
  }

  private static class IgnorePrefixComparator implements Comparator<Result> {

    private final static KVComparator comparator = new KVComparator();
    private int prefixLength;

    public IgnorePrefixComparator(int prefixLength) {
      this.prefixLength = prefixLength;
    }

    @Override
    public int compare(Result r1, Result r2) {
      if (null == r1 || null == r2) {
        throw new IllegalArgumentException();
      }
      return comparator.compareIgnoringPrefix(prefixLength,
          r1.getRow(), 0, r1.getRow().length,
          r2.getRow(), 0, r2.getRow().length);
    }
  }

  private class ScannerIterator implements Iterator<Result> {
    private ResultScanner scanner;
    private Result next = null;
    private boolean closed = false;

    protected ScannerIterator(ResultScanner scanner) {
      this.scanner = scanner;
    }

    public boolean isClosed() {
      return closed;
    }

    public void close() {
      if (closed)
        return;
      this.scanner.close();
      this.closed = true;
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        try {
          next = scanner.next();
          if (next == null) {
            close();
          }
          return next != null;
        } catch (IOException e) {
          MergeSortScanner.this.close();
          throw new RuntimeException(e);
        }
      }
      return true;
    }

    @Override
    public Result next() {
      // since hasNext() does the real advancing, we call this to determine
      // if there is a next before proceeding.
      if (!hasNext()) {
        return null;
      }

      // if we get to here, then hasNext() has given us an item to return.
      // we want to return the item and then null out the next pointer, so
      // we use a temporary variable.
      Result temp = next;
      next = null;
      return temp;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
