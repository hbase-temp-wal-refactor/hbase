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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * MetaData provider for the given WAL implementation
 *
 * It provides facilities that
 *   check existence of certain WAL
 *   retrieve WAL entities under given namespace
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WALMetaDataProvider {

  /**
   * @param log complete name of the log to check if it exists or not
   * @return
   * @throws IOException
   */
  boolean exists(String log) throws IOException;

  /**
   * @param WALIdentity it could be a namespace for a Stream or directory/path for a FS based storage
   * @return
   * @throws IOException
   */
  WALIdentity[] list(WALIdentity WALIdentity) throws IOException;

}
