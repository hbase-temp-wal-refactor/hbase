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
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WALInfo extends Comparable<WALInfo> {

  WALInfo UNKNOWN = new WALInfo() {
    
    @Override
    public long getWalStartTime() {
      return 0;
    }
    
    @Override
    public long getSize() throws IOException{
      return 0;
    }
    
    @Override
    public String getName() {
      return "UNKNOWN";
    }

    @Override
    public Path getPath() {
      return null;
    }

    @Override
    public int compareTo(WALInfo o) {
      return 0;
    }
  };

  /**
   * For the FS based path, it will be just a file name of whole path
   * For stream based, it will be name of the stream 
   * @return name of the wal
   */
  String getName();

  /**
   * Starting time of the wal which help in sorting against the others
   * @return start time of the wal
   */
  long getWalStartTime();

  /**
   * Used for getting the size of the Wal
   * @return size of the log stream or file
   * @throws IOException
   */
  long getSize() throws IOException;

  /**
   * Used in case of FSBased system only
   * @return 
   */
  Path getPath();
  
}
