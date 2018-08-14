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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSWalInfo implements WALInfo{
  private static final Pattern WAL_FILE_NAME_PATTERN =
      Pattern.compile("(.+)\\.(\\d+)(\\.[0-9A-Za-z]+)?");
  private String name;
  private Path path;

  public FSWalInfo(String name) {
    this.path = new Path(name);
    if (path != null) {
      this.name = path.getName();
    }
  }
  
  public FSWalInfo(Path path) {
    this.path = path;
    if(path !=null){
      this.name = path.getName();
      }
  }
  
  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getWalStartTime() {
    return Long.parseLong(getWALNameGroupFromWALName(name, 2));
  }

  private static String getWALNameGroupFromWALName(String name, int group) {
    Matcher matcher = WAL_FILE_NAME_PATTERN.matcher(name);
    if (matcher.matches()) {
      return matcher.group(group);
    } else {
      throw new IllegalArgumentException(name + " is not a valid wal file name");
    }
  }

  @Override
  public long getSize() throws IOException {
    // TODO Implement WALInfo.getSize
    return -1;
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public int compareTo(WALInfo o) {
    FSWalInfo that = (FSWalInfo)o;
    return this.path.compareTo(that.getPath());
  }
  
  @Override
  public String toString() {
   return this.path.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FSWalInfo)) {
      return false;
    }
    FSWalInfo that = (FSWalInfo) obj;
    return this.path.equals(that.getPath());
  }
}
