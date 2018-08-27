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

import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.DEFAULT_WAL_TRAILER_WARN_SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.WAL_TRAILER_WARN_SIZE;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Protobuf log writer.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class AbstractFSProtobufLogWriter extends AbstractProtobufLogWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSProtobufLogWriter.class);

  private boolean initializeCompressionContext(Configuration conf, Path path) throws IOException {
    boolean doCompress = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    if (doCompress) {
      try {
        this.compressionContext = new CompressionContext(LRUDictionary.class,
            FSUtils.isRecoveredEdits(path),
            conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true));
      } catch (Exception e) {
        throw new IOException("Failed to initiate CompressionContext", e);
      }
    }
    return doCompress;
  }

  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable,
      long blocksize) throws IOException, StreamLacksCapabilityException {
    this.conf = conf;
    boolean doCompress = initializeCompressionContext(conf, path);
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
    int bufferSize = FSUtils.getDefaultBufferSize(fs);
    short replication = (short) conf.getInt("hbase.regionserver.hlog.replication",
      FSUtils.getDefaultReplication(fs, path));

    initOutput(fs, path, overwritable, bufferSize, replication, blocksize);

    boolean doTagCompress = doCompress
        && conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
    length.set(writeMagicAndWALHeader(ProtobufLogReader.PB_WAL_MAGIC, buildWALHeader(conf,
      WALHeader.newBuilder().setHasCompression(doCompress).setHasTagCompression(doTagCompress))));

    initAfterHeader(doCompress);

    // instantiate trailer to default value.
    trailer = WALTrailer.newBuilder().build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Initialized protobuf WAL=" + path + ", compression=" + doCompress);
    }
  }

  private void initAfterHeader0(boolean doCompress) throws IOException {
    WALCellCodec codec = getCodec(conf, this.compressionContext);
    this.cellEncoder = codec.getEncoder(getOutputStreamForCellEncoder());
    if (doCompress) {
      this.compressor = codec.getByteStringCompressor();
    } else {
      this.compressor = WALCellCodec.getNoneCompressor();
    }
  }

  protected void initAfterHeader(boolean doCompress) throws IOException {
    initAfterHeader0(doCompress);
  }

  // should be called in sub classes's initAfterHeader method to init SecureWALCellCodec.
  protected final void secureInitAfterHeader(boolean doCompress, Encryptor encryptor)
      throws IOException {
    if (conf.getBoolean(HConstants.ENABLE_WAL_ENCRYPTION, false) && encryptor != null) {
      WALCellCodec codec = SecureWALCellCodec.getCodec(this.conf, encryptor);
      this.cellEncoder = codec.getEncoder(getOutputStreamForCellEncoder());
      // We do not support compression
      this.compressionContext = null;
      this.compressor = WALCellCodec.getNoneCompressor();
    } else {
      initAfterHeader0(doCompress);
    }
  }

  protected abstract void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
      short replication, long blockSize) throws IOException, StreamLacksCapabilityException;
}
