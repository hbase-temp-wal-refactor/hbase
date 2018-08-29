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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.ListWal;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WalInfoImpl;
import org.apache.hadoop.hbase.replication.regionserver.AbstractWALEntryStream;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.RecoveredReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.WALEntryStream;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ListWalProvider implements WALProvider {

  private Configuration conf;
  private String providerId;
  protected List<WALActionsListener> listeners = new ArrayList<>();
  protected AtomicBoolean initialized = new AtomicBoolean(false);
  private String logPrefix;
  private final Object walCreateLock = new Object();
  public static final String WAL_FILE_NAME_DELIMITER = ".";
  public static final String META_WAL_PROVIDER_ID = ".meta";
  protected volatile ListWal wal;
  private ListWalMetaDataProvider listWalMetaDataProvider;

  @Override
  public void init(WALFactory factory, Configuration conf, String providerId) throws IOException {
    if (!initialized.compareAndSet(false, true)) {
      throw new IllegalStateException("WALProvider.init should only be called once.");
    }
    this.conf = conf;
    this.providerId = providerId;
    // get log prefix
    StringBuilder sb = new StringBuilder().append(factory.factoryId);
    if (providerId != null) {
      if (providerId.startsWith(WAL_FILE_NAME_DELIMITER)) {
        sb.append(providerId);
      } else {
        sb.append(WAL_FILE_NAME_DELIMITER).append(providerId);
      }
    }
    logPrefix = sb.toString();
    listWalMetaDataProvider = new ListWalMetaDataProvider();
  }

  @Override
  public WAL getWAL(RegionInfo region) throws IOException {
    ListWal walCopy = wal;
    if (walCopy == null) {
      // only lock when need to create wal, and need to lock since
      // creating hlog on fs is time consuming
      synchronized (walCreateLock) {
        walCopy = wal;
        if (walCopy == null) {
          walCopy = createWAL();
          boolean succ = false;
          try {
            walCopy.init();
            succ = true;
          } finally {
            if (!succ) {
              walCopy.close();
            }
          }
          wal = walCopy;
        }
      }
    }
    return walCopy;
  }

  private ListWal createWAL() throws IOException {
    return new ListWal(conf, listWalMetaDataProvider, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null,
        listeners);
  }

  @Override
  public List<WAL> getWALs() {
    if (wal == null) {
      return Collections.emptyList();
    }
    List<WAL> wals = new ArrayList<>(1);
    wals.add(wal);
    return wals;
  }

  @Override
  public void shutdown() throws IOException {
    WAL log = this.wal;
    if (log != null) {
      log.shutdown();
    }
  }

  @Override
  public void close() throws IOException {
    WAL log = this.wal;
    if (log != null) {
      log.close();
    }
  }

  @Override
  public long getNumLogFiles() {
    ListWal log = this.wal;
    return log == null ? 0 : log.getNumLogFiles();
  }

  /**
   * iff the given WALFactory is using the DefaultWALProvider for meta and/or non-meta, count the
   * size of files (only rolled). if either of them aren't, count 0 for that provider.
   */
  @Override
  public long getLogFileSize() {
    ListWal log = this.wal;
    return log == null ? 0 : log.getLogFileSize();
  }

  @Override
  public void addWALActionsListener(WALActionsListener listener) {
    listeners.add(listener);
  }

  @Override
  public WALEntryStream getWalStream(PriorityBlockingQueue<WALInfo> logQueue, Configuration conf,
      long startPosition, WALFileLengthProvider walFileLengthProvider, ServerName serverName,
      MetricsSource metrics) throws IOException {
    return new AbstractWALEntryStream(logQueue, conf, startPosition, walFileLengthProvider,
        serverName, metrics) {

      @Override
      protected void handleIOException(WALInfo walInfo, IOException e) throws IOException {
        throw e;
      }

      @Override
      protected Reader createReader(WALInfo walInfo, Configuration conf) throws IOException {
        return listWalMetaDataProvider.createReader(walInfo);
      }
    };
  }

  @Override
  public WALMetaDataProvider getWalMetaDataTracker() throws IOException {
    return listWalMetaDataProvider;
  }

  public class ListWalMetaDataProvider implements WALMetaDataProvider {
    ConcurrentHashMap<WALInfo, List<Entry>> map = new ConcurrentHashMap<WALInfo, List<Entry>>();

    @Override
    public boolean exists(String log) throws IOException {
      return map.containsKey(new WalInfoImpl(log));
    }

    public Reader createReader(WALInfo walInfo) {
      return new ListWal.ListReader(walInfo, this);
    }

    @Override
    public WALInfo[] list(WALInfo walInfo) throws IOException {
      WALInfo[] walInfos = new WALInfo[1];
      walInfos[0] = walInfo;
      return walInfos;
    }

    public List<Entry> createList(WALInfo info) {
      List<Entry> list = map.putIfAbsent(info, new ArrayList<Entry>());
      if (list == null) {
        return map.get(info);
      }
      return list;
    }

    public void clear() {
      map.clear();
    }

    public int getNumLogFiles() {
      return map.keySet().size();
    }

    public List<Entry> get(WALInfo walInfo) {
      return map.get(walInfo);
    }

  }

  @Override
  public WALInfo createWalInfo(String wal) {
    return new WalInfoImpl(wal);
  }

  @Override
  public RecoveredReplicationSource getRecoveredReplicationSource() {
    return new RecoveredReplicationSource() {
      @Override
      public void locateRecoveredPaths(PriorityBlockingQueue<WALInfo> queue) throws IOException {

      }
    };
  }

  @Override
  public WALInfo getWalFromArchivePath(String wal) {
    return null;
  }

  @Override
  public WALInfo getFullPath(ServerName serverName, String wal) {
    return new WalInfoImpl(wal);
  }

}
