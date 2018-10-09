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

import java.io.IOException;

import org.apache.hadoop.hbase.wal.WALIdentity;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class WALIdentityImpl implements WALIdentity {

  private String name;
  
  public WALIdentityImpl(String name) {
    this.name=name;
  }

  @Override
  public int compareTo(WALIdentity o) {
    return this.getName().compareTo(o.getName());
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public long getWalStartTime() {
    // TODO Implement WALIdentity.getWalStartTime
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj instanceof WALIdentity) {
      WALIdentity info = (WALIdentity) obj;
      if (this.name.equals(info.getName())) return true;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return this.name.hashCode();
  }

}
