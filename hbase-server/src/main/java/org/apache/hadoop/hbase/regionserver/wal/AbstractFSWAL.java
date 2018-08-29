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
import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.MemoryType;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.FSWALInfo;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALInfo;
import org.apache.hadoop.hbase.wal.WALMetaDataProvider;
import org.apache.hadoop.hbase.wal.WALPrettyPrinter;
import org.apache.hadoop.hbase.wal.WALProvider.WriterBase;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.htrace.core.TraceScope;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link WAL} to go against {@link FileSystem}; i.e. keep WALs in HDFS. Only one
 * WAL is ever being written at a time. When a WAL hits a configured maximum size, it is rolled.
 * This is done internal to the implementation.
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
 * To read an WAL, call
 * {@link WALFactory#createReader(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.fs.Path)}. *
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
public abstract class AbstractFSWAL<W extends WriterBase> extends AbstractWAL<W> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWAL.class);

  /**
   * file system instance
   */
  protected final FileSystem fs;

  /**
   * WAL directory, where all WAL files would be placed.
   */
  protected final Path walDir;

  /**
   * dir path where old logs are kept.
   */
  protected final Path walArchiveDir;

  /**
   * Matches just those wal files that belong to this wal instance.
   */
  protected final PathFilter ourFiles;

  /**
   * Prefix used when checking for wal membership.
   */
  protected final String prefixPathStr;

  // If > than this size, roll the log.
  protected long logrollsize;

  // Last time to check low replication on hlog's pipeline
  private long lastTimeCheckLowReplication = EnvironmentEdgeManager.currentTime();

  /**
   * WAL Comparator; it compares the timestamp (log filenum), present in the log file name. Throws
   * an IllegalArgumentException if used to compare paths from different wals.
   */
  final Comparator<Path> LOG_NAME_COMPARATOR =
    (o1, o2) -> Long.compare(getFileNumFromFileName(o1), getFileNumFromFileName(o2));

  /**
   * Map of WAL log file to properties. The map is sorted by the log file creation timestamp
   * (contained in the log file name).
   */
  protected ConcurrentNavigableMap<Path, WalProps> walFile2Props =
    new ConcurrentSkipListMap<>(LOG_NAME_COMPARATOR);

  protected AbstractFSWAL(final WALMetaDataProvider metaDataProvider, final FileSystem fs,
      final Path rootDir, final String logDir,
      final String archiveDir, final Configuration conf, final List<WALActionsListener> listeners,
      final boolean failIfWALExists, final String prefix, final String suffix)
      throws FailedLogCloseException, IOException {
    super(conf, listeners, prefix, suffix);
    this.walDir = new Path(rootDir, logDir);
    this.walArchiveDir = new Path(rootDir, archiveDir);
    this.fs = fs;
    if (!fs.exists(walDir) && !fs.mkdirs(walDir)) {
      throw new IOException("Unable to mkdir " + walDir);
    }

    if (!fs.exists(this.walArchiveDir)) {
      if (!fs.mkdirs(this.walArchiveDir)) {
        throw new IOException("Unable to mkdir " + this.walArchiveDir);
      }
    }

    this.prefixPathStr = new Path(walDir, walFilePrefix + WAL_FILE_NAME_DELIMITER).toString();

    this.ourFiles = new PathFilter() {
      @Override
      public boolean accept(final Path fileName) {
        // The path should start with dir/<prefix> and end with our suffix
        final String fileNameString = fileName.toString();
        if (!fileNameString.startsWith(prefixPathStr)) {
          return false;
        }
        if (walFileSuffix.isEmpty()) {
          // in the case of the null suffix, we need to ensure the filename ends with a timestamp.
          return org.apache.commons.lang3.StringUtils
              .isNumeric(fileNameString.substring(prefixPathStr.length()));
        } else if (!fileNameString.endsWith(walFileSuffix)) {
          return false;
        }
        return true;
      }
    };
    // If prefix is null||empty then just name it wal
    this.walFilePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    // we only correctly differentiate suffices when numeric ones start with '.'
    if (suffix != null && !(suffix.isEmpty()) && !(suffix.startsWith(WAL_FILE_NAME_DELIMITER))) {
      throw new IllegalArgumentException("WAL suffix must start with '" + WAL_FILE_NAME_DELIMITER +
        "' but instead was '" + suffix + "'");
    }
    // Now that it exists, set the storage policy for the entire directory of wal files related to
    // this FSHLog instance
    String storagePolicy =
        conf.get(HConstants.WAL_STORAGE_POLICY, HConstants.DEFAULT_WAL_STORAGE_POLICY);
    CommonFSUtils.setStoragePolicy(fs, this.walDir, storagePolicy);

    if (failIfWALExists) {
      final FileStatus[] walFiles = CommonFSUtils.listStatus(fs, walDir, ourFiles);
      if (null != walFiles && 0 != walFiles.length) {
        throw new IOException("Target WAL already exists within directory " + walDir);
      }
    }

    // Schedule a WAL roll when the WAL is 50% of the HDFS block size. Scheduling at 50% of block
    // size should make it so WAL rolls before we get to the end-of-block (Block transitions cost
    // some latency). In hbase-1 we did this differently. We scheduled a roll when we hit 95% of
    // the block size but experience from the field has it that this was not enough time for the
    // roll to happen before end-of-block. So the new accounting makes WALs of about the same
    // size as those made in hbase-1 (to prevent surprise), we now have default block size as
    // 2 times the DFS default: i.e. 2 * DFS default block size rolling at 50% full will generally
    // make similar size logs to 1 * DFS default block size rolling at 95% full. See HBASE-19148.
    this.blocksize = WALUtil.getWALBlockSize(this.conf, this.fs, this.walDir);
    float multiplier = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.5f);
    this.logrollsize = (long)(this.blocksize * multiplier);

    boolean maxLogsDefined = conf.get("hbase.regionserver.maxlogs") != null;
    if (maxLogsDefined) {
      LOG.warn("'hbase.regionserver.maxlogs' was deprecated.");
    }
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs",
      Math.max(32, calculateMaxLogFiles(conf, logrollsize)));
  }

  /**
   * This is a convenience method that computes a new filename with a given file-number.
   * @param filenum to use
   * @return Path
   */
  protected Path computeFilename(final long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("WAL file number can't be < 0");
    }
    String child = walFilePrefix + WAL_FILE_NAME_DELIMITER + filenum + walFileSuffix;
    return new Path(walDir, child);
  }

  /**
   * This is a convenience method that computes a new filename with a given using the current WAL
   * file-number
   * @return Path
   */
  public Path getCurrentFileName() {
    return computeFilename(this.filenum.get());
  }

  @VisibleForTesting
  Path getOldPath() {
    long currentFilenum = this.filenum.get();
    Path oldPath = null;
    if (currentFilenum > 0) {
      // ComputeFilename will take care of meta wal filename
      oldPath = computeFilename(currentFilenum);
    } // I presume if currentFilenum is <= 0, this is first file and null for oldPath if fine?
    return oldPath;
  }

  /**
   * if the given {@code path} is being written currently, then return its length.
   * <p>
   * This is used by replication to prevent replicating unacked log entries. See
   * https://issues.apache.org/jira/browse/HBASE-14004 for more details.
   */
  @Override
  public OptionalLong getLogFileSizeIfBeingWritten(WALInfo path) {
    rollWriterLock.lock();
    try {
      Path currentPath = getOldPath();
      if (path instanceof FSWALInfo &&
          ((FSWALInfo)path).getPath().equals(currentPath)) {
        W writer = this.writer;
        return writer != null ? OptionalLong.of(writer.getLength()) : OptionalLong.empty();
      } else {
        return OptionalLong.empty();
      }
    } finally {
      rollWriterLock.unlock();
    }
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of rolled log files */
  public int getNumRolledLogFiles() {
    return walFile2Props.size();
  }

  // public only until class moves to o.a.h.h.wal
  /** @return the number of log files in use */
  public int getNumLogFiles() {
    // +1 for current use log
    return getNumRolledLogFiles() + 1;
  }

  /**
   * If the number of un-archived WAL files is greater than maximum allowed, check the first
   * (oldest) WAL file, and returns those regions which should be flushed so that it can be
   * archived.
   * @return regions (encodedRegionNames) to flush in order to archive oldest WAL file.
   */
  byte[][] findRegionsToForceFlush() throws IOException {
    byte[][] regions = null;
    int logCount = getNumRolledLogFiles();
    if (logCount > this.maxLogs && logCount > 0) {
      Map.Entry<Path, WalProps> firstWALEntry = this.walFile2Props.firstEntry();
      regions =
        this.sequenceIdAccounting.findLower(firstWALEntry.getValue().encodedName2HighestSequenceId);
    }
    if (regions != null) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < regions.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(Bytes.toStringBinary(regions[i]));
      }
      LOG.info("Too many WALs; count=" + logCount + ", max=" + this.maxLogs +
        "; forcing flush of " + regions.length + " regions(s): " + sb.toString());
    }
    return regions;
  }

  protected final void logRollAndSetupWalProps(Path oldPath, Path newPath, long oldFileLen) {
    int oldNumEntries = this.numEntries.getAndSet(0);
    String newPathString = newPath != null ? CommonFSUtils.getPath(newPath) : null;
    if (oldPath != null) {
      this.walFile2Props.put(oldPath,
        new WalProps(this.sequenceIdAccounting.resetHighest(), oldFileLen));
      this.totalLogSize.addAndGet(oldFileLen);
      LOG.info("Rolled WAL {} with entries={}, filesize={}; new WAL {}",
        CommonFSUtils.getPath(oldPath), oldNumEntries, StringUtils.byteDesc(oldFileLen),
        newPathString);
    } else {
      LOG.info("New WAL {}", newPathString);
    }
  }

  /**
   * A log file has a creation timestamp (in ms) in its file name ({@link #filenum}. This helper
   * method returns the creation timestamp from a given log file. It extracts the timestamp assuming
   * the filename is created with the {@link #computeFilename(long filenum)} method.
   * @return timestamp, as in the log file name.
   */
  protected long getFileNumFromFileName(Path fileName) {
    checkNotNull(fileName, "file name can't be null");
    if (!ourFiles.accept(fileName)) {
      throw new IllegalArgumentException(
          "The log file " + fileName + " doesn't belong to this WAL. (" + toString() + ")");
    }
    final String fileNameString = fileName.toString();
    String chompedPath = fileNameString.substring(prefixPathStr.length(),
      (fileNameString.length() - walFileSuffix.length()));
    return Long.parseLong(chompedPath);
  }

  private int calculateMaxLogFiles(Configuration conf, long logRollSize) {
    Pair<Long, MemoryType> globalMemstoreSize = MemorySizeUtil.getGlobalMemStoreSize(conf);
    return (int) ((globalMemstoreSize.getFirst() * 2) / logRollSize);
  }

  /**
   * retrieve the next path to use for writing. Increments the internal filenum.
   */
  private Path getNewPath() throws IOException {
    this.filenum.set(System.currentTimeMillis());
    Path newPath = getCurrentFileName();
    while (fs.exists(newPath)) {
      this.filenum.incrementAndGet();
      newPath = getCurrentFileName();
    }
    return newPath;
  }

  private void archiveLogFile(final Path p) throws IOException {
    Path newPath = getWALArchivePath(this.walArchiveDir, p);
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (Object i : this.listeners) {
        ((WALActionsListener)i).preLogArchive(new FSWALInfo(p), new FSWALInfo(newPath));
      }
    }
    LOG.info("Archiving " + p + " to " + newPath);
    if (!CommonFSUtils.renameAndSetModifyTime(this.fs, p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (Object i : this.listeners) {
        ((WALActionsListener)i).postLogArchive(new FSWALInfo(p), new FSWALInfo(newPath));
      }
    }
  }

  /**
   * Archive old logs. A WAL is eligible for archiving if all its WALEdits have been flushed.
   */
  protected void cleanOldLogs() throws IOException {
    List<Pair<Path, Long>> logsToArchive = null;
    // For each log file, look at its Map of regions to highest sequence id; if all sequence ids
    // are older than what is currently in memory, the WAL can be GC'd.
    for (Object o : this.walFile2Props.entrySet()) {
      Map.Entry<Path, WalProps> e = (Map.Entry<Path, WalProps>) o;
      Path log = e.getKey();
      Map<byte[], Long> sequenceNums = e.getValue().encodedName2HighestSequenceId;
      if (this.sequenceIdAccounting.areAllLower(sequenceNums)) {
        if (logsToArchive == null) {
          logsToArchive = new ArrayList<>();
        }
        logsToArchive.add(Pair.newPair(log, e.getValue().logSize));
        if (LOG.isTraceEnabled()) {
          LOG.trace("WAL file ready for archiving " + log);
        }
      }
    }
    if (logsToArchive != null) {
      for (Pair<Path, Long> logAndSize : logsToArchive) {
        this.totalLogSize.addAndGet(-logAndSize.getSecond());
        archiveLogFile(logAndSize.getFirst());
        this.walFile2Props.remove(logAndSize.getFirst());
      }
    }
  }

  @Override
  public byte[][] rollWriter(boolean force) throws FailedLogCloseException, IOException {
    rollWriterLock.lock();
    try {
      if (this.closed) {
        throw new WALClosedException("WAL has been closed");
      }
      // Return if nothing to flush.
      if (!force && this.writer != null && this.numEntries.get() <= 0) {
        return null;
      }
      byte[][] regionsToFlush = null;
      try (TraceScope scope = TraceUtil.createTrace("FSHLog.rollWriter")) {
        Path oldPath = getOldPath();
        Path newPath = getNewPath();
        // Any exception from here on is catastrophic, non-recoverable so we currently abort.
        W nextWriter = (W) this.createWriterInstance(newPath);
        tellListenersAboutPreLogRoll(new FSWALInfo(oldPath), new FSWALInfo(newPath));
        // NewPath could be equal to oldPath if replaceWriter fails.
        newPath = replaceWriter(oldPath, newPath, nextWriter);
        tellListenersAboutPostLogRoll(new FSWALInfo(oldPath), new FSWALInfo(newPath));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Create new " + implClassName + " writer with pipeline: " +
            Arrays.toString(getPipeline()));
        }
        // Can we delete any of the old log files?
        if (getNumRolledLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = findRegionsToForceFlush();
        }
      } catch (CommonFSUtils.StreamLacksCapabilityException exception) {
        // If the underlying FileSystem can't do what we ask, treat as IO failure so
        // we'll abort.
        throw new IOException(
            "Underlying FileSystem can't meet stream requirements. See RS log " + "for details.",
            exception);
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  /**
   * Get the backing files associated with this WAL.
   * @return may be null if there are no files.
   */
  @VisibleForTesting
  FileStatus[] getFiles() throws IOException {
    return CommonFSUtils.listStatus(fs, walDir, ourFiles);
  }

  /*
   * only public so WALSplitter can use.
   * @return archived location of a WAL file with the given path p
   */
  public static Path getWALArchivePath(Path archiveDir, Path p) {
    return new Path(archiveDir, p.getName());
  }

  @Override
  public void close() throws IOException {
    shutdown();
    final FileStatus[] files = getFiles();
    if (null != files && 0 != files.length) {
      for (FileStatus file : files) {
        Path p = getWALArchivePath(this.walArchiveDir, file.getPath());
        // Tell our listeners that a log is going to be archived.
        if (!this.listeners.isEmpty()) {
          for (Object i : this.listeners) {
            ((WALActionsListener)i).preLogArchive(new FSWALInfo(file.getPath()), new FSWALInfo(p));
          }
        }

        if (!CommonFSUtils.renameAndSetModifyTime(fs, file.getPath(), p)) {
          throw new IOException("Unable to rename " + file.getPath() + " to " + p);
        }
        // Tell our listeners that a log was archived.
        if (!this.listeners.isEmpty()) {
          for (Object i : this.listeners) {
            ((WALActionsListener)i).postLogArchive(new FSWALInfo(file.getPath()), new FSWALInfo(p));
          }
        }
      }
      LOG.debug(
        "Moved " + files.length + " WAL file(s) to " + CommonFSUtils.getPath(this.walArchiveDir));
    }
    LOG.info("Closed WAL: " + toString());
  }

  private static void split(final Configuration conf, final Path p) throws IOException {
    FileSystem fs = FSUtils.getWALFileSystem(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.getFileStatus(p).isDirectory()) {
      throw new IOException(p + " is not a directory");
    }

    final Path baseDir = FSUtils.getWALRootDir(conf);
    Path archiveDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (conf.getBoolean(AbstractFSWALProvider.SEPARATE_OLDLOGDIR,
      AbstractFSWALProvider.DEFAULT_SEPARATE_OLDLOGDIR)) {
      archiveDir = new Path(archiveDir, p.getName());
    }
    WALSplitter.split(baseDir, p, archiveDir, fs, conf, WALFactory.getInstance(conf));
  }

  private static void usage() {
    System.err.println("Usage: AbstractFSWAL <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: " +
      "AbstractFSWAL --dump hdfs://example.com:9000/hbase/WALs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println(
      "         For example: AbstractFSWAL --split hdfs://example.com:9000/hbase/WALs/DIR");
  }

  /**
   * Pass one or more log file names and it will either dump out a text version on
   * <code>stdout</code> or split the specified log files.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the WALPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      WALPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--perf") == 0) {
      LOG.error(HBaseMarkers.FATAL, "Please use the WALPerformanceEvaluation tool instead. i.e.:");
      LOG.error(HBaseMarkers.FATAL,
        "\thbase org.apache.hadoop.hbase.wal.WALPerformanceEvaluation --iterations " + args[1]);
      System.exit(-1);
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          Path logPath = new Path(args[i]);
          FSUtils.setFsDefault(conf, logPath);
          split(conf, logPath);
        } catch (IOException t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }

  protected abstract W createWriterInstance(Path path)
      throws IOException, CommonFSUtils.StreamLacksCapabilityException;

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
   * This method gets the pipeline for the current WAL.
   */
  @VisibleForTesting
  abstract DatanodeInfo[] getPipeline();

  protected final void postSync(final long timeInNanos, final int handlerSyncs) {
    if (timeInNanos > this.slowSyncNs) {
      String msg = new StringBuilder().append("Slow sync cost: ").append(timeInNanos / 1000000)
          .append(" ms, current pipeline: ").append(Arrays.toString(getPipeline())).toString();
      TraceUtil.addTimelineAnnotation(msg);
      LOG.info(msg);
    }
    if (!listeners.isEmpty()) {
      for (WALActionsListener listener : listeners) {
        listener.postSync(timeInNanos, handlerSyncs);
      }
    }
  }

  /**
   * This method gets the datanode replication count for the current WAL.
   */
  @VisibleForTesting
  abstract int getLogReplication();
}
