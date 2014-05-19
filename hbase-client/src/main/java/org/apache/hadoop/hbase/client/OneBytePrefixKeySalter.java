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

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * This will prepend one byte before the rowkey
 * The prepended byte is the hash value of the original row key.
 *
 */
public class OneBytePrefixKeySalter extends NBytePrefixKeySalter {
  private final static int ONE_BYTE = 1;
  private int slots;

  public OneBytePrefixKeySalter() {
    this(256);
  }

  public OneBytePrefixKeySalter(int limit) {
    super(ONE_BYTE);
    this.slots = limit;
  }

  protected byte[] hash(byte[] key) {
    byte[] result = new byte[ONE_BYTE];
    result[0] = 0x00;
    int hash = 1;
    if (key == null || key.length == 0) {
      return result;
    }
    for (int i = 0; i < key.length; i++) {
      hash = 31 * hash + (int)(key[i]);
    }
    hash = hash & 0x7fffffff;
    result[0] = (byte)(hash % slots);
    return result;
  }

  @Override
  public byte[][] getAllSalts() {

    byte[][] salts = new byte[slots][];
    for (int i = 0; i < salts.length; i++) {
      salts[i] = new byte[]{(byte)i};
    }
    Arrays.sort(salts, Bytes.BYTES_RAWCOMPARATOR);
    return salts;
  }
}