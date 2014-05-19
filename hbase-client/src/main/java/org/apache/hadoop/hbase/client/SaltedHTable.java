package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * This operator is used to access(get, delete, put, scan) salted table easily.
 *
 */
public class SaltedHTable implements HTableInterface{

  private KeySalter salter;
  private HTableInterface table;

  public SaltedHTable(HTableInterface table, KeySalter salter) {
    this.table = table;
    this.salter = salter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(Get get) throws IOException {
    return unSalt(table.get(salt(get)));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
     return getScanner(scan, null);
  }

  /**
   * Allow to scan on specified salts.
   * @param scan
   * @param salts
   * @return
   * @throws IOException
   */
  public ResultScanner getScanner(Scan scan, byte[][] salts) throws IOException {
    return new SaltedScanner(scan, salts, false);
 }

 /**
   * Allow to scan on specified salts.
   * @param scan
   * @param salts
   * @param keepSalt, whether to keep the salt in the key.
   * @return
   * @throws IOException
   */
  public ResultScanner getScanner(Scan scan, byte[][] salts, boolean keepSalt) throws IOException {
    return new SaltedScanner(scan, salts, keepSalt);
 }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(Put put) throws IOException {
    table.put(salt(put));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(Delete delete) throws IOException {
    table.delete(salt(delete));
  }

  private Get salt(Get get) throws IOException {
    if (null == get) {
      return null;
    }
    Get newGet = new Get(salter.salt(get.getRow()));
    newGet.setFilter(get.getFilter());
    newGet.setCacheBlocks(get.getCacheBlocks());
    newGet.setMaxVersions(get.getMaxVersions());
    newGet.setTimeRange(get.getTimeRange().getMin(), get.getTimeRange().getMax());
    newGet.getFamilyMap().putAll(get.getFamilyMap());
    return newGet;
  }

  private Delete salt(Delete delete) {
    if (null == delete) {
      return null;
    }
    byte[] newRow = salter.salt(delete.getRow());
    Delete newDelete = new Delete(newRow);

    Map<byte[], List<Cell>> newMap = salt(delete.getFamilyCellMap());
    newDelete.getFamilyCellMap().putAll(newMap);
    return newDelete;
  }

  private Put salt(Put put) {
    if (null == put) {
      return null;
    }
    byte[] newRow = salter.salt(put.row);
    Put newPut = new Put(newRow, put.ts);
    Map<byte[], List<Cell>> newMap = salt(put.getFamilyCellMap());
    newPut.getFamilyCellMap().putAll(newMap);
    newPut.durability = put.durability;
    for (Map.Entry<String, byte[]> entry : put.getAttributesMap().entrySet()) {
      newPut.setAttribute(entry.getKey(), entry.getValue());
    }
    return newPut;
  }

  private Map<byte[], List<Cell>> salt(Map<byte[], List<Cell>> familyMap) {
    if (null == familyMap) {
      return null;
    }
    Map<byte[], List<Cell>> result = new HashMap<byte[], List<Cell>>();
    for (Map.Entry<byte[], List<Cell>> entry :
      familyMap.entrySet()) {
      List<Cell> kvs = entry.getValue();
      if (null != kvs) {
        List<Cell> newKvs = new ArrayList<Cell>();
        for (int i = 0; i < kvs.size(); i++) {
          newKvs.add(salt(kvs.get(i)));
        }
        result.put(entry.getKey(), newKvs);
      }
    }
    return result;
  }

  private KeyValue salt(Cell cell) {
    if (null == cell) {
      return null;
    }
    KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
    byte[] newRow = salter.salt(kv.getRow());
    return new KeyValue(newRow, 0,
        newRow.length,
        kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getType()),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

  private Result unSalt(Result result) {
    if (null == result) {
      return null;
    }
    KeyValue[] results = result.raw();
    if (null == results) {
      return null;
    }
    KeyValue[] newResults = new KeyValue[results.length];

    for (int i = 0; i < results.length; i++) {
      newResults[i] = unSalt(results[i]);
    }
    return new Result(newResults);
  }

  private KeyValue unSalt(KeyValue kv) {
    if (null == kv) {
      return null;
    }
    byte[] newRowKey = salter.unSalt(kv.getRow());
    return new KeyValue(newRowKey, 0, newRowKey.length,
        kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength(),
        kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
        kv.getTimestamp(), KeyValue.Type.codeToType(kv.getType()),
        kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
  }

  public HTableInterface getRawTable() {
    return this.table;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[] getTableName() {
    return table.getTableName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Configuration getConfiguration() {
    return table.getConfiguration();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    return table.getTableDescriptor();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAutoFlush() {
    return table.isAutoFlush();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flushCommits() throws IOException {
    table.flushCommits();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    table.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean exists(Get get) throws IOException {
    Get newGet = salt(get);
    return table.exists(newGet);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {
    if (null == gets || gets.size() == 0) {
      return null;
    }
    Result[] result = new Result[gets.size()];
    for (int i = 0; i < gets.size(); i++) {
      Get newGet = salt(gets.get(i));
      result[i] = unSalt(table.get(newGet));
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(List<Put> puts) throws IOException {
    if (null == puts || puts.size() == 0) {
      return;
    }
    List<Put> newPuts = new ArrayList<Put>(puts.size());
    for (int i = 0; i < puts.size(); i++) {
      newPuts.add(salt(puts.get(i)));
    }
    table.put(newPuts);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(List<Delete> deletes) throws IOException {
    if (null == deletes ||deletes.size() == 0) {
      return;
    }
    List<Delete> newDeletes = new ArrayList<Delete>(deletes.size());
    for (int i = 0; i < deletes.size(); i++) {
      newDeletes.add(salt(deletes.get(i)));
    }
    table.delete(newDeletes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result append(Append append) throws IOException {
    Result result = table.append(salt(append));
    return unSalt(result);
  }

  private Append salt(Append append) {
    if (null == append) {
      return null;
    }
    byte[] newRow = salter.salt(append.getRow());
    Append newAppend = new Append(newRow);

    Map<byte[], List<Cell>> newMap = salt(append.getFamilyCellMap());
    newAppend.getFamilyCellMap().putAll(newMap);
    return newAppend;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    byte[] newRow = salter.salt(row);
    Result result = table.getRowOrBefore(newRow, family);
    return unSalt(result);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return getScanner(scan);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    byte[] newRow = salter.salt(row);
    Put newPut = salt(put);
    return table.checkAndPut(newRow, family, qualifier, value, newPut);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndPut(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareFilter.CompareOp compareOp, final byte [] value,
      final Put put) throws IOException {
    byte[] newRow = salter.salt(row);
    Put newPut = salt(put);
    return table.checkAndPut(newRow, family, qualifier, compareOp, value, newPut);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    byte[] newRow = salter.salt(row);
    Delete newDelete = salt(delete);
    return table.checkAndDelete(newRow, family, qualifier, value, newDelete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkAndDelete(final byte [] row, final byte [] family,
      final byte [] qualifier, final CompareFilter.CompareOp compareOp, final byte [] value,
      final Delete delete) throws IOException {
    byte[] newRow = salter.salt(row);
    Delete newDelete = salt(delete);
    return table.checkAndDelete(newRow, family, qualifier, compareOp, value, newDelete);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount, writeToWAL);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(Increment increment) throws IOException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void batch(List<? extends Row> actions, Object[] results)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] batch(List<? extends Row> actions) throws IOException,
      InterruptedException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    // checkState();
    // return table.coprocessorService(row);
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Batch.Call<T, R> callable)
      throws ServiceException, Throwable {
    // checkState();
    // return table.coprocessorService(service, startKey, endKey, callable);
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Callback<R> callback)
      throws ServiceException, Throwable {
    // checkState();
    // table.coprocessorService(service, startKey, endKey, callable, callback);
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * This scanner will merge sort the scan result, and remove the salts
   *
   */
  private class SaltedScanner implements ResultScanner {

    private MergeSortScanner scanner;
    private boolean keepSalt;

    public SaltedScanner (Scan scan, byte[][] salts, boolean keepSalt) throws IOException {
      Scan[] scans = salt(scan, salts);
      this.keepSalt = keepSalt;
      this.scanner = new MergeSortScanner(scans, table,
          salter.getSaltLength());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Result> iterator() {
      return new Iterator<Result>() {

        public boolean hasNext() {
          return scanner.iterator().hasNext();
        }

        public Result next() {
          if (keepSalt) {
            return scanner.iterator().next();
          }
          else {
            return unSalt(scanner.iterator().next());
          }
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result next() throws IOException {
      if (keepSalt) {
        return scanner.next();
      }
      else {
        return unSalt(scanner.next());
      }
    }

     /**
      * {@inheritDoc}
      */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
      scanner.close();
    }

    private Scan[] salt(Scan scan, byte[][] salts) throws IOException {

      byte[][] splits = null;
      if (null != salts) {
        splits = salts;
      }
      else {
        splits = salter.getAllSalts();
      }
      Scan[] scans = new Scan[splits.length];
      byte[] start = scan.getStartRow();
      byte[] end = scan.getStopRow();

      for (int i = 0; i < splits.length; i++) {
        scans[i] = new Scan(scan);
        scans[i].setStartRow(concat(splits[i], start));
        if (end.length == 0) {
          scans[i].setStopRow( (i == splits.length - 1) ?
              HConstants.EMPTY_END_ROW : splits[i + 1]);
        }
        else {
          scans[i].setStopRow(concat(splits[i], end));
        }
      }
      return scans;
    }

    private byte[] concat(byte[] prefix, byte[] row) {
      if (null == prefix || prefix.length == 0) {
        return row;
      }
      if (null == row || row.length == 0) {
        return prefix;
      }
      byte[] newRow = new byte[row.length + prefix.length];
      if (row.length != 0) {
        System.arraycopy(row, 0, newRow, prefix.length, row.length);
      }
      if (prefix.length != 0) {
        System.arraycopy(prefix, 0, newRow, 0, prefix.length);
      }
      return newRow;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
    table.setAutoFlush(autoFlush, clearBufferOnFail);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    table.setWriteBufferSize(writeBufferSize);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getWriteBufferSize() {
    return table.getWriteBufferSize();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TableName getName() {
    return table.getName();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Boolean[] exists(List<Get> gets) throws IOException {
    if (gets.isEmpty()) return new Boolean[]{};
    if (gets.size() == 1) return new Boolean[]{exists(gets.get(0))};

    Boolean[] results = new Boolean[gets.size()];
    int i = 0;
    for (Get g: gets){
      Get newGet = salt(g);
      results[i++] = table.exists(newGet);
    }
    return results;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R> Object[] batchCallback(List<? extends Row> actions, Callback<R> callback)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount,
      Durability durability) throws IOException {
    byte[] newRow = salter.salt(row);
    return table.incrementColumnValue(newRow, family, qualifier, amount, durability);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlush(boolean autoFlush) {
    setAutoFlush(autoFlush, autoFlush);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAutoFlushTo(boolean autoFlush) {
    table.setAutoFlushTo(autoFlush);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor methodDescriptor, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                          Message request, byte[] startKey, byte[] endKey, R responsePrototype,
                                                          Batch.Callback<R> callback) throws ServiceException, Throwable {
    throw new UnsupportedOperationException("Please use getRawTable to get underlying table");
  }
}
