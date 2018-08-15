/**
 *
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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.wal.FSWalInfo;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALInfo;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming access to WAL entries. This class is given a queue of WAL {@link Path}, and continually
 * iterates through all the WAL {@link Entry} in the queue. When it's done reading from a Path, it
 * dequeues it and starts reading from the next.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSWALEntryStream extends AbstractWALEntryStream {
  private static final Logger LOG = LoggerFactory.getLogger(FSWALEntryStream.class);

  private FileSystem fs;

  public FSWALEntryStream(FileSystem fs, PriorityBlockingQueue<WALInfo> logQueue, Configuration conf,
      long startPosition, WALFileLengthProvider walFileLengthProvider, ServerName serverName,
      MetricsSource metrics) throws IOException {
    super(logQueue, conf, startPosition, walFileLengthProvider, serverName, metrics);
    this.fs = fs;
  }

  @Override
  // HBASE-15984 check to see we have in fact parsed all data in a cleanly closed file
  protected boolean checkAllBytesParsed() throws IOException {
    // -1 means the wal wasn't closed cleanly.
    final long trailerSize = currentTrailerSize();
    FileStatus stat = null;
    try {
      stat = fs.getFileStatus(this.currentPath.getPath());
    } catch (IOException exception) {
      LOG.warn("Couldn't get file length information about log {}, it {} closed cleanly {}",
        currentPath, trailerSize < 0 ? "was not" : "was", getCurrentPathStat());
      metrics.incrUnknownFileLengthForClosedWAL();
    }
    // Here we use currentPositionOfReader instead of currentPositionOfEntry.
    // We only call this method when currentEntry is null so usually they are the same, but there
    // are two exceptions. One is we have nothing in the file but only a header, in this way
    // the currentPositionOfEntry will always be 0 since we have no change to update it. The other
    // is that we reach the end of file, then currentPositionOfEntry will point to the tail of the
    // last valid entry, and the currentPositionOfReader will usually point to the end of the file.
    if (stat != null) {
      if (trailerSize < 0) {
        if (currentPositionOfReader < stat.getLen()) {
          final long skippedBytes = stat.getLen() - currentPositionOfReader;
          LOG.debug(
            "Reached the end of WAL file '{}'. It was not closed cleanly,"
                + " so we did not parse {} bytes of data. This is normally ok.",
            currentPath, skippedBytes);
          metrics.incrUncleanlyClosedWALs();
          metrics.incrBytesSkippedInUncleanlyClosedWALs(skippedBytes);
        }
      } else if (currentPositionOfReader + trailerSize < stat.getLen()) {
        LOG.warn(
          "Processing end of WAL file '{}'. At position {}, which is too far away from"
              + " reported file length {}. Restarting WAL reading (see HBASE-15983 for details). {}",
          currentPath, currentPositionOfReader, stat.getLen(), getCurrentPathStat());
        setPosition(0);
        resetReader();
        metrics.incrRestartedWALReading();
        metrics.incrRepeatedFileBytes(currentPositionOfReader);
        return false;
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Reached the end of log " + this.currentPath + ", and the length of the file is "
          + (stat == null ? "N/A" : stat.getLen()));
    }
    metrics.incrCompletedWAL();
    return true;
  }

  private Path getArchivedLog(Path path) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);

    // Try found the log in old dir
    Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path archivedLogLocation = new Path(oldLogDir, path.getName());
    if (fs.exists(archivedLogLocation)) {
      LOG.info("Log " + path + " was moved to " + archivedLogLocation);
      return archivedLogLocation;
    }

    // Try found the log in the seperate old log dir
    oldLogDir = new Path(rootDir, new StringBuilder(HConstants.HREGION_OLDLOGDIR_NAME)
        .append(Path.SEPARATOR).append(serverName.getServerName()).toString());
    archivedLogLocation = new Path(oldLogDir, path.getName());
    if (fs.exists(archivedLogLocation)) {
      LOG.info("Log " + path + " was moved to " + archivedLogLocation);
      return archivedLogLocation;
    }

    LOG.error("Couldn't locate log: " + path);
    return path;
  }

  private void handleFileNotFound(Path path, FileNotFoundException fnfe) throws IOException {
    // If the log was archived, continue reading from there
    Path archivedLog = getArchivedLog(path);
    if (!path.equals(archivedLog)) {
      openReader(new FSWalInfo(archivedLog));
    } else {
      throw fnfe;
    }
  }

  // For HBASE-15019
  private void recoverLease(final Configuration conf, final Path path) {
    try {
      final FileSystem dfs = FSUtils.getCurrentFileSystem(conf);
      FSUtils fsUtils = FSUtils.getInstance(dfs, conf);
      fsUtils.recoverFileLease(dfs, path, conf, new CancelableProgressable() {
        @Override
        public boolean progress() {
          LOG.debug("recover WAL lease: " + path);
          return true;
        }
      });
    } catch (IOException e) {
      LOG.warn("unable to recover lease for WAL: " + path, e);
    }
  }

  @Override
  protected void handleIOException(WALInfo path, IOException e) throws IOException {
    try {
      throw e;
    } catch (FileNotFoundException fnfe) {
      handleFileNotFound(path.getPath(), fnfe);
    } catch (EOFException eo) {
      handleEofException(eo);
    } catch (LeaseNotRecoveredException lnre) {
      // HBASE-15019 the WAL was not closed due to some hiccup.
      LOG.warn("Try to recover the WAL lease " + currentPath, lnre);
      recoverLease(conf, currentPath.getPath());
      reader = null;
    }
  }

  private long currentTrailerSize() {
    long size = -1L;
    if (reader instanceof ProtobufLogReader) {
      final ProtobufLogReader pblr = (ProtobufLogReader) reader;
      size = pblr.trailerSize();
    }
    return size;
  }

  // if we get an EOF due to a zero-length log, and there are other logs in queue
  // (highly likely we've closed the current log), we've hit the max retries, and autorecovery is
  // enabled, then dump the log
  private void handleEofException(IOException e) {
    if ((e instanceof EOFException || e.getCause() instanceof EOFException) && logQueue.size() > 1
        && this.eofAutoRecovery) {
      try {
        if (fs.getFileStatus(logQueue.peek().getPath()).getLen() == 0) {
          LOG.warn("Forcing removal of 0 length log in queue: " + logQueue.peek());
          logQueue.remove();
          setPosition(0);
        }
      } catch (IOException ioe) {
        LOG.warn("Couldn't get file length information about log " + logQueue.peek());
      }
    }
  }

  private String getCurrentPathStat() {
    StringBuilder sb = new StringBuilder();
    if (currentPath != null) {
      sb.append("currently replicating from: ").append(currentPath).append(" at position: ")
          .append(currentPositionOfEntry).append("\n");
    } else {
      sb.append("no replication ongoing, waiting for new log");
    }
    return sb.toString();
  }

  @Override
  protected Reader createReader(WALInfo path, Configuration conf) throws IOException {
    return WALFactory.createReader(fs, path.getPath(), conf);
  }

}
