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
package org.apache.hadoop.hbase.replication.regionserver;


import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.FSWALIdentity;
import org.apache.hadoop.hbase.wal.WALIdentity;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class FSRecoveredReplicationSource extends RecoveredReplicationSource {
  
  private static final Logger LOG = LoggerFactory.getLogger(FSRecoveredReplicationSource.class);
  private String logDir;
  
  @Override
  public void init(Configuration conf, ReplicationSourceManager manager,
      ReplicationQueueStorage queueStorage, ReplicationPeer replicationPeer, Server server,
      String peerClusterZnode, UUID clusterId, WALFileSizeProvider walFileSizeProvider,
      MetricsSource metrics, WALProvider walProvider) throws IOException {
    super.init(conf, manager, queueStorage, replicationPeer, server, peerClusterZnode, clusterId,
      walFileSizeProvider, metrics, walProvider);
    this.logDir = AbstractFSWALProvider.getWALDirectoryName(server.getServerName().toString());
  }
  
  @Override
  public void locateRecoveredWALIdentities(PriorityBlockingQueue<WALIdentity> queue)
      throws IOException {
    boolean hasWALIdentityChanged = false;
    PriorityBlockingQueue<WALIdentity> newWALIdentities =
        new PriorityBlockingQueue<WALIdentity>(queueSizePerGroup, new LogsComparator());
    WALIdentityLoop:
    for (WALIdentity WALIdentity : queue) {
      if (walProvider.getWalMetaDataProvider()
          .exists(((FSWALIdentity)WALIdentity).getPath().toString())) {
        // still in same location, don't need to do anything
        newWALIdentities.add(WALIdentity);
        continue;
      }
      // WALIdentity changed - try to find the right WALIdentity.
      hasWALIdentityChanged = true;
      if (server instanceof ReplicationSyncUp.DummyServer) {
        // In the case of disaster/recovery, HMaster may be shutdown/crashed before flush data
        // from .logs to .oldlogs. Loop into .logs folders and check whether a match exists
        WALIdentity newWALIdentity = getReplSyncUpPath(WALIdentity);
        newWALIdentities.add(newWALIdentity);
        continue;
      } else {
        // See if Path exists in the dead RS folder (there could be a chain of failures
        // to look at)
        List<ServerName> deadRegionServers = this.replicationQueueInfo.getDeadRegionServers();
        LOG.info("NB dead servers : " + deadRegionServers.size());
        final Path walDir = FSUtils.getWALRootDir(conf);
        for (ServerName curDeadServerName : deadRegionServers) {
          final Path deadRsDirectory =
              new Path(walDir, AbstractFSWALProvider.getWALDirectoryName(curDeadServerName
                  .getServerName()));
          Path[] locs = new Path[] { new Path(deadRsDirectory, WALIdentity.getName()), new Path(
              deadRsDirectory.suffix(AbstractFSWALProvider.SPLITTING_EXT), WALIdentity.getName()) };
          for (Path possibleLogLocation : locs) {
            LOG.info("Possible location " + possibleLogLocation.toUri().toString());
            if (walProvider.getWalMetaDataProvider().exists(possibleLogLocation.toString())) {
              // We found the right new location
              LOG.info("Log " + WALIdentity + " still exists at " + possibleLogLocation);
              newWALIdentities.add(new FSWALIdentity(possibleLogLocation));
              continue WALIdentityLoop;
            }
          }
        }
        // didn't find a new location
        LOG.error(
          String.format("WAL Path %s doesn't exist and couldn't find its new location", WALIdentity));
        newWALIdentities.add(WALIdentity);
      }
    }

    if (hasWALIdentityChanged) {
      if (newWALIdentities.size() != queue.size()) { // this shouldn't happen
        LOG.error("Recovery queue size is incorrect");
        throw new IOException("Recovery queue size error");
      }
      // put the correct locations in the queue
      // since this is a recovered queue with no new incoming logs,
      // there shouldn't be any concurrency issues
      queue.clear();
      for (WALIdentity WALIdentity : newWALIdentities) {
        queue.add(WALIdentity);
      }
    }
  }

  // N.B. the ReplicationSyncUp tool sets the manager.getWALDir to the root of the wal
  // area rather than to the wal area for a particular region server.
  private WALIdentity getReplSyncUpPath(WALIdentity path) throws IOException {
    WALIdentity[] rss = walProvider.getWalMetaDataProvider().list(
        this.walProvider.createWALIdentity(logDir));
    for (WALIdentity rs : rss) {
      WALIdentity[] logs = walProvider.getWalMetaDataProvider().list(rs);
      for (WALIdentity log : logs) {
        WALIdentity p = this.walProvider.createWALIdentity(
            new Path(((FSWALIdentity)rs).getPath(), log.getName()).toString());
        if (p.getName().equals(path.getName())) {
          LOG.info("Log " + p.getName() + " found at " + p);
          return p;
        }
      }
    }
    LOG.error("Didn't find path for: " + path.getName());
    return path;
  }
}
