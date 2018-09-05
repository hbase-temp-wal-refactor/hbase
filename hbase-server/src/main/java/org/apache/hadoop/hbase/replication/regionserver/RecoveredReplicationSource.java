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
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.wal.WALInfo;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class that handles the recovered source of a replication stream, which is transfered from
 * another dead region server. This will be closed when all logs are pushed to peer cluster.
 */
@InterfaceAudience.Private
public abstract class RecoveredReplicationSource extends ReplicationSource {

  private String actualPeerId;

  @Override
  public void init(Configuration conf, ReplicationSourceManager manager,
      ReplicationQueueStorage queueStorage, ReplicationPeer replicationPeer, Server server,
      String peerClusterZnode, UUID clusterId, WALFileSizeProvider walFileSizeProvider,
      MetricsSource metrics, WALProvider walProvider) throws IOException {
    super.init(conf, manager, queueStorage, replicationPeer, server, peerClusterZnode,
      clusterId, walFileSizeProvider, metrics, walProvider);
    this.actualPeerId = this.replicationQueueInfo.getPeerId();
  }

  @Override
  protected RecoveredReplicationSourceShipper createNewShipper(String walGroupId,
      PriorityBlockingQueue<WALInfo> queue) {
    return new RecoveredReplicationSourceShipper(conf, walGroupId, queue, this, queueStorage);
  }

  void tryFinish() {
    if (workerThreads.isEmpty()) {
      this.getSourceMetrics().clear();
      manager.finishRecoveredSource(this);
    }
  }

  @Override
  public String getPeerId() {
    return this.actualPeerId;
  }

  @Override
  public ServerName getServerWALsBelongTo() {
    return this.replicationQueueInfo.getDeadRegionServers().get(0);
  }

  @Override
  public boolean isRecovered() {
    return true;
  }
  

  /**
   * Get the updated queue of the wals if the wals are moved to another location.
   * @param queue Updated queue with the new WalInfo(paths or stream) if wals are archived 
   * @throws IOException
   */
  public abstract void locateRecoveredWALInfos(PriorityBlockingQueue<WALInfo> queue)
      throws IOException;
}
