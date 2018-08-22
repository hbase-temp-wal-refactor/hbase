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
import java.io.OutputStream;
import java.security.Key;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.EncryptionTest;

/**
 * Base class for Protobuf log writer.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class AbstractProtobufLogWriter {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProtobufLogWriter.class);

  protected CompressionContext compressionContext;
  protected Configuration conf;
  protected Codec.Encoder cellEncoder;
  protected WALCellCodec.ByteStringCompressor compressor;
  protected boolean trailerWritten;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;

  protected AtomicLong length = new AtomicLong();

  protected WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    return WALCellCodec.create(conf, null, compressionContext);
  }

  private WALHeader buildWALHeader0(Configuration conf, WALHeader.Builder builder) {
    if (!builder.hasWriterClsName()) {
      builder.setWriterClsName(getWriterClassName());
    }
    if (!builder.hasCellCodecClsName()) {
      builder.setCellCodecClsName(WALCellCodec.getWALCellCodecClass(conf));
    }
    return builder.build();
  }

  protected WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    return buildWALHeader0(conf, builder);
  }

  // should be called in sub classes's buildWALHeader method to build WALHeader for secure
  // environment. Do not forget to override the setEncryptor method as it will be called in this
  // method to init your encryptor.
  protected final WALHeader buildSecureWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    builder.setWriterClsName(getWriterClassName());
    if (conf.getBoolean(HConstants.ENABLE_WAL_ENCRYPTION, false)) {
      EncryptionTest.testKeyProvider(conf);
      EncryptionTest.testCipherProvider(conf);

      // Get an instance of our cipher
      final String cipherName =
          conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Cipher cipher = Encryption.getCipher(conf, cipherName);
      if (cipher == null) {
        throw new RuntimeException("Cipher '" + cipherName + "' is not available");
      }

      // Generate an encryption key for this WAL
      SecureRandom rng = new SecureRandom();
      byte[] keyBytes = new byte[cipher.getKeyLength()];
      rng.nextBytes(keyBytes);
      Key key = new SecretKeySpec(keyBytes, cipher.getName());
      builder.setEncryptionKey(UnsafeByteOperations.unsafeWrap(EncryptionUtil.wrapKey(conf,
          conf.get(HConstants.CRYPTO_WAL_KEY_NAME_CONF_KEY,
              conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
                  User.getCurrent().getShortName())),
          key)));

      // Set up the encryptor
      Encryptor encryptor = cipher.getEncryptor();
      encryptor.setKey(key);
      setEncryptor(encryptor);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Initialized secure protobuf WAL: cipher=" + cipher.getName());
      }
    }
    builder.setCellCodecClsName(SecureWALCellCodec.class.getName());
    return buildWALHeader0(conf, builder);
  }

  // override this if you need a encryptor
  protected void setEncryptor(Encryptor encryptor) {
  }

  protected String getWriterClassName() {
    return getClass().getSimpleName();
  }

  void setWALTrailer(WALTrailer walTrailer) {
    this.trailer = walTrailer;
  }

  public long getLength() {
    return length.get();
  }

  private WALTrailer buildWALTrailer(WALTrailer.Builder builder) {
    return builder.build();
  }

  protected void writeWALTrailer() {
    try {
      int trailerSize = 0;
      if (this.trailer == null) {
        // use default trailer.
        LOG.warn("WALTrailer is null. Continuing with default.");
        this.trailer = buildWALTrailer(WALTrailer.newBuilder());
        trailerSize = this.trailer.getSerializedSize();
      } else if ((trailerSize = this.trailer.getSerializedSize()) > this.trailerWarnSize) {
        // continue writing after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum size : " + trailerSize
            + " > " + this.trailerWarnSize);
      }
      length.set(writeWALTrailerAndMagic(trailer, ProtobufLogReader.PB_WAL_COMPLETE_MAGIC));
      this.trailerWritten = true;
    } catch (IOException ioe) {
      LOG.warn("Failed to write trailer, non-fatal, continuing...", ioe);
    }
  }

  /**
   * return the file length after written.
   */
  protected abstract long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException;

  protected abstract long writeWALTrailerAndMagic(WALTrailer trailer, byte[] magic)
      throws IOException;

  protected abstract OutputStream getOutputStreamForCellEncoder();
}
