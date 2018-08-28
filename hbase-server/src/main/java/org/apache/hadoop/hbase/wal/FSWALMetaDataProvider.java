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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSWALMetaDataProvider implements WALMetaDataProvider {

  private FileSystem fs;

  public FSWALMetaDataProvider(FileSystem walFileSystem) {
    this.fs=walFileSystem;
  }

  @Override
  public boolean exists(String logLocation) throws IOException {
    return fs.exists(new Path(logLocation));
  }

  @Override
  public WALInfo[] list(WALInfo logDir) throws IOException {
    FileStatus[] listStatus = fs.listStatus(((FSWALInfo)logDir).getPath());
    WALInfo[] walInfos = new FSWALInfo[listStatus.length];
    for (FileStatus fileStatus : listStatus) {
      walInfos[0] = new FSWALInfo(fileStatus.getPath());
    }
    return walInfos;
  }

}
