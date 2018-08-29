/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl.WriteEntry;
import org.apache.hadoop.hbase.wal.ListWalProvider.ListWalMetaDataProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALInfo;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALMetaDataProvider;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ListWal implements WAL {

  protected final AtomicBoolean shutdown = new AtomicBoolean(false);
  protected final AtomicLong filenum = new AtomicLong(-1);
  protected final SequenceIdAccounting sequenceIdAccounting = new SequenceIdAccounting();
  List<Entry> activeWal;
  private WALCoprocessorHost coprocessorHost;
  protected final List<WALActionsListener> listeners = new CopyOnWriteArrayList<>();
  private String walFilePrefix;
  private String walFileSuffix;
  private boolean closed;
  private ListWalMetaDataProvider metaDataProvider;

  public ListWal(Configuration conf, WALMetaDataProvider metaDataProvider,
      String prefix, String suffix,
      List<WALActionsListener> listeners) throws IOException {
    this.coprocessorHost = new WALCoprocessorHost(this, conf);
    // If prefix is null||empty then just name it wal
    this.walFilePrefix =
        prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER
          + "' but instead was '" + suffix + "'");
    }
    this.walFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");
    this.metaDataProvider = (ListWalMetaDataProvider)metaDataProvider;
    // Register listeners. TODO: Should this exist anymore? We have CPs?
    if (listeners != null) {
      for (WALActionsListener i : listeners) {
        registerWALActionsListener(i);
      }
    }
  }

  @Override
  public OptionalLong getLogFileSizeIfBeingWritten(WALInfo walInfo) {
    return OptionalLong.of(metaDataProvider.get(walInfo).size());
  }

  @Override
  public byte[][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  @Override
  public byte[][] rollWriter(boolean force) throws FailedLogCloseException, IOException {
    if (this.closed) {
      throw new WALClosedException("WAL has been closed");
    }
    WALInfo oldPath = getOldPath();
    WALInfo newPath = getNewPath();
    tellListenersAboutPreLogRoll(oldPath, newPath);
    activeWal = createWal(newPath);
    tellListenersAboutPostLogRoll(oldPath, newPath);
    return null;
  }

  List<Entry> createWal(WALInfo newPath) {
    return metaDataProvider.createList(newPath);
  }

  @Override
  public void shutdown() throws IOException {
    if (!shutdown.compareAndSet(false, true)) {
      return;
    }
    closed = true;
    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logCloseRequested();
      }
    }
    metaDataProvider.clear();
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }

  @Override
  public long append(RegionInfo info, WALKeyImpl key, WALEdit edits, boolean inMemstore)
      throws IOException {
    if (this.closed) {
      throw new IOException(
          "Cannot append; log is closed, regionName = " + key.getEncodedRegionName());
    }
    Entry entry = new Entry(key, edits);
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin();
    stampRegionSequenceId(entry,we, inMemstore);
    activeWal.add(entry);
    return activeWal.size();
  }

  
  
  private void stampRegionSequenceId(Entry entry, WriteEntry we, boolean inMemstore) throws IOException {
    long regionSequenceId = we.getWriteNumber();
    if (!entry.getEdit().isReplay() && inMemstore) {
      for (Cell c : entry.getEdit().getCells()) {
        PrivateCellUtil.setSequenceId(c, regionSequenceId);
      }
    }
    entry.getKey().setWriteEntry(we);
  }

  @Override
  public void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceid,
      boolean onlyIfGreater) {
    sequenceIdAccounting.updateStore(encodedRegionName, familyName, sequenceid, onlyIfGreater);
  }

  @Override
  public void sync() throws IOException {

  }

  @Override
  public void sync(long txid) throws IOException {
  }

  /**
   * Used to initialize the WAL. Usually just call rollWriter to create the first log writer.
   */
  public void init() throws IOException {
    rollWriter();
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, families);
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Map<byte[], Long> familyToSeq) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, familyToSeq);
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.completeCacheFlush(encodedRegionName);
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName) {
    // Used by tests. Deprecated as too subtle for general usage.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName, familyName);
  }

  /**
   * This is a convenience method that computes a new filename with a given file-number.
   * @param filenum to use
   * @return WALInfo
   */
  protected WALInfo computeLogName(final long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("WAL file number can't be < 0");
    }
    String child = walFilePrefix + WAL_FILE_NAME_DELIMITER + filenum + walFileSuffix;
    return new WalInfoImpl(child);
  }

  /**
   * This is a convenience method that computes a new filename with a given using the current WAL
   * file-number
   * @return WALInfo
   */
  public WALInfo getCurrentFileName() {
    return computeLogName(this.filenum.get());
  }

  /**
   * retrieve the next path to use for writing. Increments the internal filenum.
   */
  private WALInfo getNewPath() throws IOException {
    this.filenum.set(System.currentTimeMillis());
    WALInfo newPath = getCurrentFileName();
    while (metaDataProvider.exists(newPath.getName())) {
      this.filenum.incrementAndGet();
      newPath = getCurrentFileName();
    }
    return newPath;
  }

  @VisibleForTesting
  WALInfo getOldPath() {
    long currentFilenum = this.filenum.get();
    WALInfo oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename will take care of meta wal filename
      oldPath = computeLogName(currentFilenum);
    } // I presume if currentFilenum is <= 0, this is first file and null for oldPath if fine?
    return oldPath;
  }

  /**
   * Tell listeners about post log roll.
   */
  private void tellListenersAboutPostLogRoll(final WALInfo oldPath, final WALInfo newPath)
      throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogRoll(oldPath, newPath);
      }
    }
    coprocessorHost.postWALRoll(oldPath, newPath);
  }

  /**
   * Tell listeners about pre log roll.
   */
  private void tellListenersAboutPreLogRoll(final WALInfo oldPath, final WALInfo newPath)
      throws IOException {
    coprocessorHost.preWALRoll(oldPath, newPath);

    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogRoll(oldPath, newPath);
      }
    }
  }

  public int getLogFileSize() {
    return activeWal.size();
  }

  public int getNumLogFiles() {
    return metaDataProvider.getNumLogFiles();
  }
  
  public static class ListReader implements Reader {

    private List<Entry> list;
    int count = 0;

    public ListReader(WALInfo info, ListWalMetaDataProvider metaDataProvider) {
      list = metaDataProvider.get(info);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Entry next() throws IOException {
      if (count >= list.size()) {
        return null;
      }
      Entry entry = list.get(count);
      ++count;
      WALKeyImpl key = entry.getKey();
      WALKeyImpl keyModify = new WALKeyImpl(key.getEncodedRegionName(), key.getTableName(),
          key.getLogSeqNum(), key.getWriteTime(), new ArrayList<>(), key.getNonceGroup(),
          key.getNonce(), key.getMvcc(), key.getReplicationScopes());
      // Modifying Key with modifiable list of UUIDs , so that replication can edit them
      return new Entry(keyModify, entry.getEdit());
    }

    @Override
    public Entry next(Entry reuse) throws IOException {
      return next();
    }

    @Override
    public void seek(long pos) throws IOException {
      count = (int)pos;
    }

    @Override
    public long getPosition() throws IOException {
      return count;
    }

    @Override
    public void reset() throws IOException {
      seek(0);
    }

  }

}