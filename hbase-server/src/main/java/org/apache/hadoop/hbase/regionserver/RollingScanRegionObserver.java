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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RollingScanConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Used for the rolling scan. If the rolling scan is enabled, after this region is opened but not
 * online, the FirstKeySortedStoreFiles will be created for each Store, where the
 * ChangedReadersObserver will be registered. In this observer, if the rolling scan is enabled, the
 * RollingStoreFileScanner will be used instead, this is done in the preStoreScannerOpen method.
 */
public class RollingScanRegionObserver extends BaseRegionObserver {

  private static final Log LOG = LogFactory.getLog(RollingScanRegionObserver.class);
  private boolean rollingScanEnabled = false;
  private Map<byte[], FirstKeySortedStoreFiles> firstKeySortedStoreFilesMap = Collections
      .emptyMap();

  /**
   * Finds out whether the rolling scan is enabled. If it's enabled, instantiate the
   * FirstKeySortedStoreFiles for the Stores in this region.
   *
   * @param e
   */
  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
    // find out whether the rolling scan is enabled.
    HRegion region = e.getEnvironment().getRegion();
    HTableDescriptor htd = region.getTableDesc();
    byte[] value = htd.getValue(Bytes.toBytes(RollingScanConstants.ROLLING_SCAN));
    if (value != null) {
      rollingScanEnabled = Boolean.valueOf(Bytes.toString(value)).booleanValue();
      if (rollingScanEnabled) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The rolling scan for the table[" + htd.getNameAsString()
              + "] is enabled");
        }
        firstKeySortedStoreFilesMap = new TreeMap<byte[], FirstKeySortedStoreFiles>(
            Bytes.BYTES_RAWCOMPARATOR);
        Map<byte[], Store> storeMap = region.getStores();
        if (!storeMap.isEmpty()) {
          for (Entry<byte[], Store> storeEntry : storeMap.entrySet()) {
            FirstKeySortedStoreFiles firstKeySortedStoreFiles = new FirstKeySortedStoreFiles(
                storeEntry.getValue());
            firstKeySortedStoreFilesMap.put(storeEntry.getKey(), firstKeySortedStoreFiles);
          }
        }
      }
    }
  }

  /**
   * If the rolling scan is enabled, use the RollingStoreFileScanner instead.
   *
   * @param c
   * @param store
   * @param scan
   * @param targetCols
   * @param s
   */
  @Override
  public KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
      Store store, Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s)
      throws IOException {
    if (rollingScanEnabled) {
      List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
      FirstKeySortedStoreFiles firstKeySortedStoreFiles = firstKeySortedStoreFilesMap.get(store
          .getFamily().getName());
      scanners.addAll(((HStore) store).memstore.getScanners(Long.MAX_VALUE));
      RollingStoreFileScanner fileScanner = new RollingStoreFileScanner(store,
          firstKeySortedStoreFiles, targetCols, scan);
      scanners.add(fileScanner);
      StoreScanner storeScanner = new StoreScanner(store, store.getScanInfo(), scan, scanners,
          ScanType.USER_SCAN, Long.MAX_VALUE, HConstants.LATEST_TIMESTAMP);

      // take control from store scanner to monitor the store file changes.
      store.deleteChangedReaderObserver(storeScanner);
      return storeScanner;
    } else {
      return s;
    }
  }

  /**
   * If the rolling scan is enabled, Called after the region is reported as closed to the master.
   * @param c the environment provided by the region server
   * @param abortRequested true if the region server is aborting
   */
  @Override
  public void postClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
    if (rollingScanEnabled) {
      if (LOG.isDebugEnabled()) {
        HTableDescriptor htd = c.getEnvironment().getRegion().getTableDesc();
        LOG.debug("The rolling scan region observer for the table["
            + htd.getNameAsString() + "] starts to close");
      }
      for (Entry<byte[], FirstKeySortedStoreFiles> firstKeySortedStoreFilesEntry : firstKeySortedStoreFilesMap
          .entrySet()) {
        try {
          firstKeySortedStoreFilesEntry.getValue().close();
        } catch (IOException e1) {
          LOG.warn(
              "Fail to close the FirstKeySortedStoreFiles for the column family "
                  + Bytes.toString(firstKeySortedStoreFilesEntry.getKey()), e1);
        }
      }
    }
  }
}