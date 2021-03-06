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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.hadoop.hbase.wal.WALIdentity;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming access to WAL entries. This class is given a queue of WAL {@link WALIdentity}, and continually
 * iterates through all the WAL {@link Entry} in the queue. When it's done reading from an Entry, it
 * dequeues and starts reading from the next Entry.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractWALEntryStream implements WALEntryStream {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWALEntryStream.class);

  protected Reader reader;
  protected WALIdentity currentPath;
  // cache of next entry for hasNext()
  protected Entry currentEntry;
  // position for the current entry. As now we support peek, which means that the upper layer may
  // choose to return before reading the current entry, so it is not safe to return the value below
  // in getPosition.
  protected long currentPositionOfEntry = 0;
  // position after reading current entry
  protected long currentPositionOfReader = 0;
  protected final PriorityBlockingQueue<WALIdentity> logQueue;
  protected final Configuration conf;
  protected final WALFileSizeProvider walFileSizeProvider;
  // which region server the WALs belong to
  protected final ServerName serverName;
  protected final MetricsSource metrics;

  protected boolean eofAutoRecovery;
  private WALProvider provider;

  /**
   * Create an entry stream over the given queue at the given start position
   * @param logQueue the queue of WAL paths
   * @param conf {@link Configuration} to use to create {@link Reader} for this stream
   * @param startPosition the position in the first WAL to start reading at
   * @param serverName the server name which all WALs belong to
   * @param metrics replication metrics
   * @throws IOException
   */
  public AbstractWALEntryStream(PriorityBlockingQueue<WALIdentity> logQueue, Configuration conf,
      long startPosition, WALFileSizeProvider walFileSizeProvider, ServerName serverName,
      MetricsSource metrics, WALProvider provider) throws IOException {
    this.logQueue = logQueue;
    this.conf = conf;
    this.currentPositionOfEntry = startPosition;
    this.walFileSizeProvider = walFileSizeProvider;
    this.serverName = serverName;
    this.metrics = metrics;
    this.eofAutoRecovery = conf.getBoolean("replication.source.eof.autorecovery", false);
    this.provider = provider;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (currentEntry == null) {
      try {
        tryAdvanceEntry();
      } catch (IOException e) {
          handleIOException(logQueue.peek(), e);
      }
    }
    return currentEntry != null;
  }

  @Override
  public Entry peek() throws IOException {
    return hasNext() ? currentEntry: null;
  }

  @Override
  public void seek(long pos) throws IOException {
    reader.seek(pos);
  }

  @Override
  public Entry next(Entry reuse) throws IOException {
    return next();
  }

  @Override
  public Entry next() throws IOException {
    Entry save = peek();
    currentPositionOfEntry = currentPositionOfReader;
    currentEntry = null;
    return save;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    closeReader();
  }

  @Override
  public WALIdentity getCurrentWALIdentity() {
    return currentPath;
  }

  @Override
  public long getPosition() {
    return currentPositionOfEntry;
  }

  @Override
  public void reset() throws IOException {
    if (reader != null && currentPath != null) {
      resetReader();
    }
  }

  protected void setPosition(long position) {
    currentPositionOfEntry = position;
  }

  abstract void setCurrentPath(WALIdentity path);

  private void tryAdvanceEntry() throws IOException {
    if (checkReader()) {
      boolean beingWritten = readNextEntryAndRecordReaderPosition();
      if (currentEntry == null && !beingWritten) {
        // no more entries in this log file, and the file is already closed, i.e, rolled
        // Before dequeueing, we should always get one more attempt at reading.
        // This is in case more entries came in after we opened the reader, and the log is rolled
        // while we were reading. See HBASE-6758
        resetReader();
        readNextEntryAndRecordReaderPosition();
        if (currentEntry == null) {
          if (checkAllBytesParsed()) { // now we're certain we're done with this log file
            dequeueCurrentLog();
            if (openNextLog()) {
              readNextEntryAndRecordReaderPosition();
            }
          }
        }
      }
      // if currentEntry != null then just return
      // if currentEntry == null but the file is still being written, then we should not switch to
      // the next log either, just return here and try next time to see if there are more entries in
      // the current file
    }
    // do nothing if we don't have a WAL Reader (e.g. if there's no logs in queue)
  }

  

  private void dequeueCurrentLog() throws IOException {
    LOG.debug("Reached the end of log {}", currentPath);
    closeReader();
    logQueue.remove();
    setPosition(0);
    metrics.decrSizeOfLogQueue();
  }

  /**
   * Returns whether the file is opened for writing.
   */
  private boolean readNextEntryAndRecordReaderPosition() throws IOException {
    Entry readEntry = reader.next();
    long readerPos = reader.getPosition();
    OptionalLong fileLength = walFileSizeProvider.getLogFileSizeIfBeingWritten(currentPath);
    if (fileLength.isPresent() && readerPos > fileLength.getAsLong()) {
      // see HBASE-14004, for AsyncFSWAL which uses fan-out, it is possible that we read uncommitted
      // data, so we need to make sure that we do not read beyond the committed file length.
      if (LOG.isDebugEnabled()) {
        LOG.debug("The provider tells us the valid length for " + currentPath + " is " +
            fileLength.getAsLong() + ", but we have advanced to " + readerPos);
      }
      resetReader();
      return true;
    }
    if (readEntry != null) {
      metrics.incrLogEditsRead();
      metrics.incrLogReadInBytes(readerPos - currentPositionOfEntry);
    }
    currentEntry = readEntry; // could be null
    this.currentPositionOfReader = readerPos;
    return fileLength.isPresent();
  }

  private void closeReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  // if we don't have a reader, open a reader on the next log
  private boolean checkReader() throws IOException {
    if (reader == null) {
      return openNextLog();
    }
    return true;
  }

  // open a reader on the next log in queue
  abstract boolean openNextLog() throws IOException;

  protected void openReader(WALIdentity path) throws IOException {
    try {
      // Detect if this is a new file, if so get a new reader else
      // reset the current reader so that we see the new data
      if (reader == null || !currentPath.equals(path)) {
        closeReader();
        reader = createReader(path, conf);
        seek();
        setCurrentPath(path);
      } else {
        resetReader();
      }
    }   catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException(FileNotFoundException.class);
      handleIOException (path, ioe);
    } catch (IOException ioe) {
      handleIOException(path, ioe);
    }
  }

  /**
   * Creates a reader for a wal info
   * 
   * @param WALIdentity path for FS based or stream name for stream based wal provider
   * @param conf 
   * @return return a reader for the file
   * @throws IOException
   */
  protected Reader createReader(WALIdentity walId, Configuration conf) throws IOException {
    return provider.createReader(walId, null, false);
  }

  protected void resetReader() throws IOException {
    try {
      currentEntry = null;
      reader.reset();
      seek();
    } catch (NullPointerException npe) {
      throw new IOException("NPE resetting reader, likely HDFS-4380", npe);
    } catch (IOException e) {
      handleIOException(currentPath, e);
    }
  }

  /**
   * Implement for handling IO exceptions , throw back if doesn't need to be handled 
   * @param WALIdentity
   * @param ioe IOException
   * @throws IOException
   */
  protected abstract void handleIOException(WALIdentity WALIdentity, IOException e) throws IOException;

  protected void seek() throws IOException {
    if (currentPositionOfEntry != 0) {
      reader.seek(currentPositionOfEntry);
    }
  }

 
  protected boolean checkAllBytesParsed() throws IOException {
    return true;
  }
  
}
