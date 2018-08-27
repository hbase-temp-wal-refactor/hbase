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
import org.apache.hadoop.hbase.wal.FSWalInfo;
import org.apache.hadoop.hbase.wal.WALInfo;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class FSRecoveredReplicationSource extends RecoveredReplicationSource{
  
  private static final Logger LOG = LoggerFactory.getLogger(FSRecoveredReplicationSource.class);
  private String logDir;
  
  @Override
  public void init(Configuration conf, ReplicationSourceManager manager,
      ReplicationQueueStorage queueStorage, ReplicationPeer replicationPeer, Server server,
      String peerClusterZnode, UUID clusterId, WALFileLengthProvider walFileLengthProvider,
      MetricsSource metrics, WALProvider walProvider) throws IOException {
    super.init(conf, manager, queueStorage, replicationPeer, server, peerClusterZnode, clusterId,
      walFileLengthProvider, metrics, walProvider);
    this.logDir = AbstractFSWALProvider.getWALDirectoryName(server.getServerName().toString());
  }
  
  @Override
  public void locateRecoveredPaths(PriorityBlockingQueue<WALInfo> queue) throws IOException {
    boolean hasPathChanged = false;
    PriorityBlockingQueue<WALInfo> newPaths =
        new PriorityBlockingQueue<WALInfo>(queueSizePerGroup, new LogsComparator());
    pathsLoop: for (WALInfo path : queue) {
      if (walProvider.getWalMetaDataTracker().exists(((FSWalInfo)path).getPath().toString())) { // still in same location, don't need to do anything
        newPaths.add(path);
        continue;
      }
      // Path changed - try to find the right path.
      hasPathChanged = true;
      if (server instanceof ReplicationSyncUp.DummyServer) {
        // In the case of disaster/recovery, HMaster may be shutdown/crashed before flush data
        // from .logs to .oldlogs. Loop into .logs folders and check whether a match exists
        WALInfo newPath = getReplSyncUpPath(path);
        newPaths.add(newPath);
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
          Path[] locs = new Path[] { new Path(deadRsDirectory, path.getName()), new Path(
              deadRsDirectory.suffix(AbstractFSWALProvider.SPLITTING_EXT), path.getName()) };
          for (Path possibleLogLocation : locs) {
            LOG.info("Possible location " + possibleLogLocation.toUri().toString());
            if (walProvider.getWalMetaDataTracker().exists(possibleLogLocation.toString())) {
              // We found the right new location
              LOG.info("Log " + path + " still exists at " + possibleLogLocation);
              newPaths.add(new FSWalInfo(possibleLogLocation));
              continue pathsLoop;
            }
          }
        }
        // didn't find a new location
        LOG.error(
          String.format("WAL Path %s doesn't exist and couldn't find its new location", path));
        newPaths.add(path);
      }
    }

    if (hasPathChanged) {
      if (newPaths.size() != queue.size()) { // this shouldn't happen
        LOG.error("Recovery queue size is incorrect");
        throw new IOException("Recovery queue size error");
      }
      // put the correct locations in the queue
      // since this is a recovered queue with no new incoming logs,
      // there shouldn't be any concurrency issues
      queue.clear();
      for (WALInfo path : newPaths) {
        queue.add(path);
      }
    }
  }

  // N.B. the ReplicationSyncUp tool sets the manager.getWALDir to the root of the wal
  // area rather than to the wal area for a particular region server.
  private WALInfo getReplSyncUpPath(WALInfo path) throws IOException {
    WALInfo[] rss = walProvider.getWalMetaDataTracker().list(this.walProvider.createWalInfo(logDir));
    for (WALInfo rs : rss) {
      WALInfo[] logs = walProvider.getWalMetaDataTracker().list(rs);
      for (WALInfo log : logs) {
        WALInfo p = this.walProvider.createWalInfo(new Path(((FSWalInfo)rs).getPath(), log.getName()).toString());
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
