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

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.WAL_FILE_NAME_DELIMITER;
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALInfo;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider.WriterBase;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.htrace.core.TraceScope;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.RingBuffer;

/**
 * <p>
 * As data is flushed from the MemStore to other on-disk structures (files sorted by key, hfiles), a
 * WAL becomes obsolete. We can let go of all the log edits/entries for a given HRegion-sequence id.
 * A bunch of work in the below is done keeping account of these region sequence ids -- what is
 * flushed out to hfiles, and what is yet in WAL and in memory only.
 * <p>
 * It is only practical to delete entire files. Thus, we delete an entire on-disk file
 * <code>F</code> when all of the edits in <code>F</code> have a log-sequence-id that's older
 * (smaller) than the most-recent flush.
 * <p>
 * <h2>Failure Semantic</h2> If an exception on append or sync, roll the WAL because the current WAL
 * is now a lame duck; any more appends or syncs will fail also with the same original exception. If
 * we have made successful appends to the WAL and we then are unable to sync them, our current
 * semantic is to return error to the client that the appends failed but also to abort the current
 * context, usually the hosting server. We need to replay the WALs. <br>
 * TODO: Change this semantic. A roll of WAL may be sufficient as long as we have flagged client
 * that the append failed. <br>
 * TODO: replication may pick up these last edits though they have been marked as failed append
 * (Need to keep our own file lengths, not rely on HDFS).
 */
@InterfaceAudience.Private
public abstract class AbstractWAL<W extends WriterBase> implements WAL {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractWAL.class);

  protected static final int DEFAULT_SLOW_SYNC_TIME_MS = 100; // in ms

  private static final int DEFAULT_WAL_SYNC_TIMEOUT_MS = 5 * 60 * 1000; // in ms, 5min

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  protected String walFilePrefix;

  /**
   * Suffix included on generated wal file names
   */
  protected final String walFileSuffix;

  protected final WALCoprocessorHost coprocessorHost;

  /**
   * conf object
   */
  protected final Configuration conf;

  /** Listeners that are called on WAL events. */
  protected final List<WALActionsListener> listeners = new CopyOnWriteArrayList<>();

  /**
   * Class that does accounting of sequenceids in WAL subsystem. Holds oldest outstanding sequence
   * id as yet not flushed as well as the most recent edit sequence id appended to the WAL. Has
   * facility for answering questions such as "Is it safe to GC a WAL?".
   */
  protected final SequenceIdAccounting sequenceIdAccounting = new SequenceIdAccounting();

  protected final long slowSyncNs;

  private final long walSyncTimeoutNs;

  /**
   * Block size to use writing files.
   */
  protected long blocksize;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit goes to disk. If too
   * many and we crash, then will take forever replaying. Keep the number of logs tidy.
   */
  protected int maxLogs;

  /**
   * This lock makes sure only one log roll runs at a time. Should not be taken while any other lock
   * is held. We don't just use synchronized because that results in bogus and tedious findbugs
   * warning when it thinks synchronized controls writer thread safety. It is held when we are
   * actually rolling the log. It is checked when we are looking to see if we should roll the log or
   * not.
   */
  protected final ReentrantLock rollWriterLock = new ReentrantLock(true);

  // The timestamp (in ms) when the log file was created.
  protected final AtomicLong filenum = new AtomicLong(-1);

  // Number of transactions in the current Wal.
  protected final AtomicInteger numEntries = new AtomicInteger(0);

  /**
   * The highest known outstanding unsync'd WALEdit transaction id. Usually, we use a queue to pass
   * WALEdit to background consumer thread, and the transaction id is the sequence number of the
   * corresponding entry in queue.
   */
  protected volatile long highestUnsyncedTxid = -1;

  /**
   * Updated to the transaction id of the last successful sync call. This can be less than
   * {@link #highestUnsyncedTxid} for case where we have an append where a sync has not yet come in
   * for it.
   */
  protected final AtomicLong highestSyncedTxid = new AtomicLong(0);

  /**
   * The total size of wal
   */
  protected final AtomicLong totalLogSize = new AtomicLong(0);
  /**
   * Current log file.
   */
  volatile W writer;

  // Last time to check low replication on hlog's pipeline
  private long lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();

  protected volatile boolean closed = false;

  protected final AtomicBoolean shutdown = new AtomicBoolean(false);
  protected static final class WalProps {

    /**
     * Map the encoded region name to the highest sequence id. Contain all the regions it has
     * entries of
     */
    public final Map<byte[], Long> encodedName2HighestSequenceId;

    /**
     * The log file size. Notice that the size may not be accurate if we do asynchronous close in
     * sub classes.
     */
    public final long logSize;

    public WalProps(Map<byte[], Long> encodedName2HighestSequenceId, long logSize) {
      this.encodedName2HighestSequenceId = encodedName2HighestSequenceId;
      this.logSize = logSize;
    }
  }

  /**
   * Map of {@link SyncFuture}s keyed by Handler objects. Used so we reuse SyncFutures.
   * <p>
   * TODO: Reuse FSWALEntry's rather than create them anew each time as we do SyncFutures here.
   * <p>
   * TODO: Add a FSWalEntry and SyncFuture as thread locals on handlers rather than have them get
   * them from this Map?
   */
  private final ConcurrentMap<Thread, SyncFuture> syncFuturesByHandler;

  /**
   * The class name of the runtime implementation, used as prefix for logging/tracing.
   * <p>
   * Performance testing shows getClass().getSimpleName() might be a bottleneck so we store it here,
   * refer to HBASE-17676 for more details
   * </p>
   */
  protected final String implClassName;

  public long getFilenum() {
    return this.filenum.get();
  }

  // must be power of 2
  protected final int getPreallocatedEventCount() {
    // Preallocate objects to use on the ring buffer. The way that appends and syncs work, we will
    // be stuck and make no progress if the buffer is filled with appends only and there is no
    // sync. If no sync, then the handlers will be outstanding just waiting on sync completion
    // before they return.
    int preallocatedEventCount =
      this.conf.getInt("hbase.regionserver.wal.disruptor.event.count", 1024 * 16);
    checkArgument(preallocatedEventCount >= 0,
      "hbase.regionserver.wal.disruptor.event.count must > 0");
    int floor = Integer.highestOneBit(preallocatedEventCount);
    if (floor == preallocatedEventCount) {
      return floor;
    }
    // max capacity is 1 << 30
    if (floor >= 1 << 29) {
      return 1 << 30;
    }
    return floor << 1;
  }

  protected AbstractWAL(
      final Configuration conf, final List<WALActionsListener> listeners,
      final boolean failIfWALExists, final String prefix, final String suffix)
      throws FailedLogCloseException, IOException {
    this.conf = conf;

    // If prefix is null||empty then just name it wal
    this.walFilePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER +
        "' but instead was '" + suffix + "'");
    }
    this.walFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");

    // Register listeners. TODO: Should this exist anymore? We have CPs?
    if (listeners != null) {
      for (WALActionsListener i : listeners) {
        registerWALActionsListener(i);
      }
    }
    this.coprocessorHost = new WALCoprocessorHost(this, conf);

    LOG.info("WAL configuration: blocksize=" + StringUtils.byteDesc(blocksize) + ", prefix=" +
      this.walFilePrefix + ", suffix=" + walFileSuffix);
    this.slowSyncNs = TimeUnit.MILLISECONDS
        .toNanos(conf.getInt("hbase.regionserver.hlog.slowsync.ms", DEFAULT_SLOW_SYNC_TIME_MS));
    this.walSyncTimeoutNs = TimeUnit.MILLISECONDS
        .toNanos(conf.getLong("hbase.regionserver.hlog.sync.timeout", DEFAULT_WAL_SYNC_TIMEOUT_MS));
    int maxHandlersCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, 200);
    // Presize our map of SyncFutures by handler objects.
    this.syncFuturesByHandler = new ConcurrentHashMap<>(maxHandlersCount);
    this.implClassName = getClass().getSimpleName();
  }

  /**
   * Used to initialize the WAL. Usually just call rollWriter to create the first log writer.
   */
  public void init() throws IOException {
    rollWriter();
  }

  @Override
  public void registerWALActionsListener(WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Set<byte[]> families) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, families);
  }

  @Override
  public Long startCacheFlush(byte[] encodedRegionName, Map<byte[], Long> familyToSeq) {
    return this.sequenceIdAccounting.startCacheFlush(encodedRegionName, familyToSeq);
  }

  @Override
  public void completeCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.completeCacheFlush(encodedRegionName);
  }

  @Override
  public void abortCacheFlush(byte[] encodedRegionName) {
    this.sequenceIdAccounting.abortCacheFlush(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName) {
    // Used by tests. Deprecated as too subtle for general usage.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName);
  }

  @Override
  public long getEarliestMemStoreSeqNum(byte[] encodedRegionName, byte[] familyName) {
    // This method is used by tests and for figuring if we should flush or not because our
    // sequenceids are too old. It is also used reporting the master our oldest sequenceid for use
    // figuring what edits can be skipped during log recovery. getEarliestMemStoreSequenceId
    // from this.sequenceIdAccounting is looking first in flushingOldestStoreSequenceIds, the
    // currently flushing sequence ids, and if anything found there, it is returning these. This is
    // the right thing to do for the reporting oldest sequenceids to master; we won't skip edits if
    // we crash during the flush. For figuring what to flush, we might get requeued if our sequence
    // id is old even though we are currently flushing. This may mean we do too much flushing.
    return this.sequenceIdAccounting.getLowestSequenceId(encodedRegionName, familyName);
  }

  @Override
  public byte[][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  /**
   * Tell listeners about pre log roll.
   */
  protected void tellListenersAboutPreLogRoll(final WALInfo oldInfo, final WALInfo newInfo)
      throws IOException {
    coprocessorHost.preWALRoll(oldInfo, newInfo);

    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogRoll(oldInfo, newInfo);
      }
    }
  }

  /**
   * Tell listeners about post log roll.
   */
  protected void tellListenersAboutPostLogRoll(final WALInfo oldPath, final WALInfo newPath)
      throws IOException {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogRoll(oldPath, newPath);
      }
    }

    coprocessorHost.postWALRoll(oldPath, newPath);
  }

  /**
   * <p>
   * Cleans up current writer closing it and then puts in place the passed in
   * <code>nextWriter</code>.
   * </p>
   * <p>
   * <ul>
   * <li>In the case of creating a new WAL, oldPath will be null.</li>
   * <li>In the case of rolling over from one file to the next, none of the parameters will be null.
   * </li>
   * <li>In the case of closing out this FSHLog with no further use newPath and nextWriter will be
   * null.</li>
   * </ul>
   * </p>
   * @param oldPath may be null
   * @param newPath may be null
   * @param nextWriter may be null
   * @return the passed in <code>newPath</code>
   * @throws IOException if there is a problem flushing or closing the underlying FS
   */
  @VisibleForTesting
  Path replaceWriter(Path oldPath, Path newPath, W nextWriter) throws IOException {
    try (TraceScope scope = TraceUtil.createTrace("FSHFile.replaceWriter")) {
      doReplaceWriter(oldPath, newPath, nextWriter);
      return newPath;
    }
  }

  protected final void blockOnSync(SyncFuture syncFuture) throws IOException {
    // Now we have published the ringbuffer, halt the current thread until we get an answer back.
    try {
      if (syncFuture != null) {
        if (closed) {
          throw new IOException("WAL has been closed");
        } else {
          syncFuture.get(walSyncTimeoutNs);
        }
      }
    } catch (TimeoutIOException tioe) {
      // SyncFuture reuse by thread, if TimeoutIOException happens, ringbuffer
      // still refer to it, so if this thread use it next time may get a wrong
      // result.
      this.syncFuturesByHandler.remove(Thread.currentThread());
      throw tioe;
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted", ie);
      throw convertInterruptedExceptionToIOException(ie);
    } catch (ExecutionException e) {
      throw ensureIOException(e.getCause());
    }
  }

  private static IOException ensureIOException(final Throwable t) {
    return (t instanceof IOException) ? (IOException) t : new IOException(t);
  }

  private IOException convertInterruptedExceptionToIOException(final InterruptedException ie) {
    Thread.currentThread().interrupt();
    IOException ioe = new InterruptedIOException();
    ioe.initCause(ie);
    return ioe;
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the size of log files in use */
  public long getLogFileSize() {
    return this.totalLogSize.get();
  }

  // public only until class moves to o.a.h.h.wal
  public void requestLogRoll() {
    requestLogRoll(false);
  }

  @Override
  public void shutdown() throws IOException {
    if (!shutdown.compareAndSet(false, true)) {
      return;
    }
    closed = true;
    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logCloseRequested();
      }
    }
    rollWriterLock.lock();
    try {
      doShutdown();
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * updates the sequence number of a specific store. depending on the flag: replaces current seq
   * number if the given seq id is bigger, or even if it is lower than existing one
   * @param encodedRegionName
   * @param familyName
   * @param sequenceid
   * @param onlyIfGreater
   */
  @Override
  public void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceid,
      boolean onlyIfGreater) {
    sequenceIdAccounting.updateStore(encodedRegionName, familyName, sequenceid, onlyIfGreater);
  }

  protected final SyncFuture getSyncFuture(long sequence) {
    return CollectionUtils
        .computeIfAbsent(syncFuturesByHandler, Thread.currentThread(), SyncFuture::new)
        .reset(sequence);
  }

  protected final void requestLogRoll(boolean tooFewReplicas) {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logRollRequested(tooFewReplicas);
      }
    }
  }

  long getUnflushedEntriesCount() {
    long highestSynced = this.highestSyncedTxid.get();
    long highestUnsynced = this.highestUnsyncedTxid;
    return highestSynced >= highestUnsynced ? 0 : highestUnsynced - highestSynced;
  }

  boolean isUnflushedEntries() {
    return getUnflushedEntriesCount() > 0;
  }

  /**
   * Exposed for testing only. Use to tricks like halt the ring buffer appending.
   */
  @VisibleForTesting
  void atHeadOfRingBufferEventHandlerAppend() {
    // Noop
  }

  protected final boolean append(W writer, FSWALEntry entry) throws IOException {
    // TODO: WORK ON MAKING THIS APPEND FASTER. DOING WAY TOO MUCH WORK WITH CPs, PBing, etc.
    atHeadOfRingBufferEventHandlerAppend();
    long start = EnvironmentEdgeManager.currentTime();
    byte[] encodedRegionName = entry.getKey().getEncodedRegionName();
    long regionSequenceId = entry.getKey().getSequenceId();

    // Edits are empty, there is nothing to append. Maybe empty when we are looking for a
    // region sequence id only, a region edit/sequence id that is not associated with an actual
    // edit. It has to go through all the rigmarole to be sure we have the right ordering.
    if (entry.getEdit().isEmpty()) {
      return false;
    }

    // Coprocessor hook.
    coprocessorHost.preWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
    if (!listeners.isEmpty()) {
      for (WALActionsListener i : listeners) {
        i.visitLogEntryBeforeWrite(entry.getKey(), entry.getEdit());
      }
    }
    doAppend(writer, entry);
    assert highestUnsyncedTxid < entry.getTxid();
    highestUnsyncedTxid = entry.getTxid();
    sequenceIdAccounting.update(encodedRegionName, entry.getFamilyNames(), regionSequenceId,
      entry.isInMemStore());
    coprocessorHost.postWALWrite(entry.getRegionInfo(), entry.getKey(), entry.getEdit());
    // Update metrics.
    postAppend(entry, EnvironmentEdgeManager.currentTime() - start);
    numEntries.incrementAndGet();
    return true;
  }

  private long postAppend(final Entry e, final long elapsedTime) throws IOException {
    long len = 0;
    if (!listeners.isEmpty()) {
      for (Cell cell : e.getEdit().getCells()) {
        len += PrivateCellUtil.estimatedSerializedSizeOf(cell);
      }
      for (WALActionsListener listener : listeners) {
        listener.postAppend(len, elapsedTime, e.getKey(), e.getEdit());
      }
    }
    return len;
  }

  protected final long stampSequenceIdAndPublishToRingBuffer(RegionInfo hri, WALKeyImpl key,
      WALEdit edits, boolean inMemstore, RingBuffer<RingBufferTruck> ringBuffer)
      throws IOException {
    if (this.closed) {
      throw new IOException(
          "Cannot append; log is closed, regionName = " + hri.getRegionNameAsString());
    }
    MutableLong txidHolder = new MutableLong();
    MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin(() -> {
      txidHolder.setValue(ringBuffer.next());
    });
    long txid = txidHolder.longValue();
    try (TraceScope scope = TraceUtil.createTrace(implClassName + ".append")) {
      FSWALEntry entry = new FSWALEntry(txid, key, edits, hri, inMemstore);
      entry.stampRegionSequenceId(we);
      ringBuffer.get(txid).load(entry);
    } finally {
      ringBuffer.publish(txid);
    }
    return txid;
  }

  @Override
  public String toString() {
    return implClassName + " " + walFilePrefix + ":" + walFileSuffix + "(num " + filenum + ")";
  }

  /**
   * NOTE: This append, at a time that is usually after this call returns, starts an mvcc
   * transaction by calling 'begin' wherein which we assign this update a sequenceid. At assignment
   * time, we stamp all the passed in Cells inside WALEdit with their sequenceId. You must
   * 'complete' the transaction this mvcc transaction by calling
   * MultiVersionConcurrencyControl#complete(...) or a variant otherwise mvcc will get stuck. Do it
   * in the finally of a try/finally block within which this append lives and any subsequent
   * operations like sync or update of memstore, etc. Get the WriteEntry to pass mvcc out of the
   * passed in WALKey <code>walKey</code> parameter. Be warned that the WriteEntry is not
   * immediately available on return from this method. It WILL be available subsequent to a sync of
   * this append; otherwise, you will just have to wait on the WriteEntry to get filled in.
   */
  @Override
  public abstract long append(RegionInfo info, WALKeyImpl key, WALEdit edits, boolean inMemstore)
      throws IOException;

  protected void doAppend(W writer, FSWALEntry entry) throws IOException {
  }

  protected abstract W createWriterInstance(Path path)
      throws IOException, CommonFSUtils.StreamLacksCapabilityException;

  protected void doReplaceWriter(Path oldPath, Path newPath, W nextWriter)
      throws IOException {
  }

  protected void doShutdown() throws IOException {
  }

  protected abstract boolean doCheckLogLowReplication();

  public void checkLogLowReplication(long checkInterval) {
    long now = EnvironmentEdgeManager.currentTime();
    if (now - lastTimeCheckLowReplication < checkInterval) {
      return;
    }
    // Will return immediately if we are in the middle of a WAL log roll currently.
    if (!rollWriterLock.tryLock()) {
      return;
    }
    try {
      lastTimeCheckLowReplication = now;
      if (doCheckLogLowReplication()) {
        requestLogRoll(true);
      }
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * This method gets the datanode replication count for the current WAL.
   */
  @VisibleForTesting
  abstract int getLogReplication();
}
