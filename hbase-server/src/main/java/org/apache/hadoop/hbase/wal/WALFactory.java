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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.wal.WAL.Reader;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Entry point for users of the Write Ahead Log.
 * Acts as the shim between internal use and the particular WALProvider we use to handle wal
 * requests.
 *
 * Configure which provider gets used with the configuration setting "hbase.wal.provider". Available
 * implementations:
 * <ul>
 *   <li><em>defaultProvider</em> : whatever provider is standard for the hbase version. Currently
 *                                  "asyncfs"</li>
 *   <li><em>asyncfs</em> : a provider that will run on top of an implementation of the Hadoop
 *                             FileSystem interface via an asynchronous client.</li>
 *   <li><em>filesystem</em> : a provider that will run on top of an implementation of the Hadoop
 *                             FileSystem interface via HDFS's synchronous DFSClient.</li>
 *   <li><em>multiwal</em> : a provider that will use multiple "filesystem" wal instances per region
 *                           server.</li>
 * </ul>
 *
 * Alternatively, you may provide a custom implementation of {@link WALProvider} by class name.
 */
@InterfaceAudience.Private
public class WALFactory {

  private static final Logger LOG = LoggerFactory.getLogger(WALFactory.class);

  /**
   * Maps between configuration names for providers and implementation classes.
   */
  static enum Providers {
    defaultProvider(AsyncFSWALProvider.class),
    filesystem(FSHLogProvider.class),
    multiwal(RegionGroupingProvider.class),
    asyncfs(AsyncFSWALProvider.class);

    final Class<? extends WALProvider> clazz;
    Providers(Class<? extends WALProvider> clazz) {
      this.clazz = clazz;
    }
  }

  public static final String WAL_PROVIDER = "hbase.wal.provider";
  static final String DEFAULT_WAL_PROVIDER = Providers.defaultProvider.name();
  public static final String WAL_PROVIDER_CLASS = "hbase.wal.provider.class";
  static final Class<? extends WALProvider> DEFAULT_WAL_PROVIDER_CLASS = AsyncFSWALProvider.class;

  public static final String META_WAL_PROVIDER = "hbase.wal.meta_provider";
  public static final String META_WAL_PROVIDER_CLASS = "hbase.wal.meta_provider.class";

  final String factoryId;
  private final WALProvider provider;
  // The meta updates are written to a different wal. If this
  // regionserver holds meta regions, then this ref will be non-null.
  // lazily intialized; most RegionServers don't deal with META
  private final AtomicReference<WALProvider> metaProvider = new AtomicReference<>();

  /**
   * Configuration-specified WAL Reader used when a custom reader is requested
   */
  private final Class<? extends AbstractFSWALProvider.Reader> logReaderClass;

  /**
   * How long to attempt opening in-recovery wals
   */
  private final int timeoutMillis;

  private final Configuration conf;

  // Used for the singleton WALFactory, see below.
  private WALFactory(Configuration conf) throws IOException {
    // this code is duplicated here so we can keep our members final.
    // until we've moved reader/writer construction down into providers, this initialization must
    // happen prior to provider initialization, in case they need to instantiate a reader/writer.
    timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
    /* TODO Both of these are probably specific to the fs wal provider */
    logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class,
      AbstractFSWALProvider.Reader.class);
    this.conf = conf;
    // end required early initialization

    // this instance can't create wals, just reader/writers.
    factoryId = SINGLETON_ID;
    if (conf.getBoolean("hbase.regionserver.hlog.enabled", true)) {
      WALProvider provider = createProvider(
          getProviderClass(WAL_PROVIDER_CLASS, WAL_PROVIDER, DEFAULT_WAL_PROVIDER));
      if (false) {
        provider = new SyncReplicationWALProvider(provider);
      }
      provider.init(this, conf, null);
      provider.addWALActionsListener(new MetricsWAL());
      this.provider = provider;
    } else {
      // special handling of existing configuration behavior.
      LOG.warn("Running with WAL disabled.");
      provider = new DisabledWALProvider();
      provider.init(this, conf, factoryId);
    }
  }

  @VisibleForTesting
  /*
   * @param clsKey config key for provider classname
   * @param key config key for provider enum
   * @param defaultValue default value for provider enum
   * @return Class which extends WALProvider
   */
  public Class<? extends WALProvider> getProviderClass(String clsKey, String key,
      String defaultValue) {
    String clsName = conf.get(clsKey);
    if (clsName == null || clsName.isEmpty()) {
      clsName = conf.get(key, defaultValue);
    }
    if (clsName != null && !clsName.isEmpty()) {
      try {
        return (Class<? extends WALProvider>) Class.forName(clsName);
      } catch (ClassNotFoundException exception) {
        // try with enum key next
      }
    }
    try {
      Providers provider = Providers.valueOf(conf.get(key, defaultValue));
      if (provider != Providers.defaultProvider) {
        // User gives a wal provider explicitly, just use that one
        return provider.clazz;
      }
      // AsyncFSWAL has better performance in most cases, and also uses less resources, we will try
      // to use it if possible. But it deeply hacks into the internal of DFSClient so will be easily
      // broken when upgrading hadoop. If it is broken, then we fall back to use FSHLog.
      if (AsyncFSWALProvider.load()) {
        return AsyncFSWALProvider.class;
      } else {
        return FSHLogProvider.class;
      }
    } catch (IllegalArgumentException exception) {
      // Fall back to them specifying a class name
      // Note that the passed default class shouldn't actually be used, since the above only fails
      // when there is a config value present.
      return conf.getClass(key, AsyncFSWALProvider.class, WALProvider.class);
    }
  }

  static WALProvider createProvider(Class<? extends WALProvider> clazz) throws IOException {
    LOG.info("Instantiating WALProvider of type {}", clazz);
    try {
      return clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("couldn't set up WALProvider, the configured class is " + clazz);
      LOG.debug("Exception details for failure to load WALProvider.", e);
      throw new IOException("couldn't set up WALProvider", e);
    }
  }

  /**
   * @param conf must not be null, will keep a reference to read params in later reader/writer
   *          instances.
   * @param factoryId a unique identifier for this factory. used i.e. by filesystem implementations
   *          to make a directory
   */
  public WALFactory(Configuration conf, String factoryId) throws IOException {
    // default enableSyncReplicationWALProvider is true, only disable SyncReplicationWALProvider
    // for HMaster or HRegionServer which take system table only. See HBASE-19999
    this(conf, factoryId, true);
  }

  /**
   * @param conf must not be null, will keep a reference to read params in later reader/writer
   *          instances.
   * @param factoryId a unique identifier for this factory. used i.e. by filesystem implementations
   *          to make a directory
   * @param enableSyncReplicationWALProvider whether wrap the wal provider to a
   *          {@link SyncReplicationWALProvider}
   */
  public WALFactory(Configuration conf, String factoryId, boolean enableSyncReplicationWALProvider)
      throws IOException {
    // until we've moved reader/writer construction down into providers, this initialization must
    // happen prior to provider initialization, in case they need to instantiate a reader/writer.
    timeoutMillis = conf.getInt("hbase.hlog.open.timeout", 300000);
    /* TODO Both of these are probably specific to the fs wal provider */
    logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl", ProtobufLogReader.class,
      AbstractFSWALProvider.Reader.class);
    this.conf = conf;
    this.factoryId = factoryId;
    // end required early initialization
    if (conf.getBoolean("hbase.regionserver.hlog.enabled", true)) {
      WALProvider provider = createProvider(
          getProviderClass(WAL_PROVIDER_CLASS, WAL_PROVIDER, DEFAULT_WAL_PROVIDER));
      if (enableSyncReplicationWALProvider) {
        provider = new SyncReplicationWALProvider(provider);
      }
      provider.init(this, conf, null);
      provider.addWALActionsListener(new MetricsWAL());
      this.provider = provider;
    } else {
      // special handling of existing configuration behavior.
      LOG.warn("Running with WAL disabled.");
      provider = new DisabledWALProvider();
      provider.init(this, conf, factoryId);
    }
  }

  /**
   * Shutdown all WALs and clean up any underlying storage.
   * Use only when you will not need to replay and edits that have gone to any wals from this
   * factory.
   */
  public void close() throws IOException {
    final WALProvider metaProvider = this.metaProvider.get();
    if (null != metaProvider) {
      metaProvider.close();
    }
    // close is called on a WALFactory with null provider in the case of contention handling
    // within the getInstance method.
    if (null != provider) {
      provider.close();
    }
  }

  /**
   * Tell the underlying WAL providers to shut down, but do not clean up underlying storage.
   * If you are not ending cleanly and will need to replay edits from this factory's wals,
   * use this method if you can as it will try to leave things as tidy as possible.
   */
  public void shutdown() throws IOException {
    IOException exception = null;
    final WALProvider metaProvider = this.metaProvider.get();
    if (null != metaProvider) {
      try {
        metaProvider.shutdown();
      } catch(IOException ioe) {
        exception = ioe;
      }
    }
    provider.shutdown();
    if (null != exception) {
      throw exception;
    }
  }

  public List<WAL> getWALs() {
    return provider.getWALs();
  }

  @VisibleForTesting
  WALProvider getMetaProvider() throws IOException {
    for (;;) {
      WALProvider provider = this.metaProvider.get();
      if (provider != null) {
        return provider;
      }
      boolean metaWALProvPresent = conf.get(META_WAL_PROVIDER_CLASS) != null;
      provider = createProvider(getProviderClass(
          metaWALProvPresent ? META_WAL_PROVIDER_CLASS : WAL_PROVIDER_CLASS,
          META_WAL_PROVIDER, conf.get(WAL_PROVIDER, DEFAULT_WAL_PROVIDER)));
      provider.init(this, conf, AbstractFSWALProvider.META_WAL_PROVIDER_ID);
      provider.addWALActionsListener(new MetricsWAL());
      if (metaProvider.compareAndSet(null, provider)) {
        return provider;
      } else {
        // someone is ahead of us, close and try again.
        provider.close();
      }
    }
  }

  /**
   * @param region the region which we want to get a WAL for it. Could be null.
   */
  public WAL getWAL(RegionInfo region) throws IOException {
    // use different WAL for hbase:meta
    if (region != null && region.isMetaRegion() &&
      region.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      return getMetaProvider().getWAL(region);
    } else {
      return provider.getWAL(region);
    }
  }

  public Reader createReader(final WALIdentity path) throws IOException {
    return createReader(path, (CancelableProgressable)null);
  }

  /**
   * Create a reader for the WAL. If you are reading from a file that's being written to and need
   * to reopen it multiple times, use {@link WAL.Reader#reset()} instead of this method
   * then just seek back to the last known good position.
   * @return A WAL reader.  Close when done with it.
   * @throws IOException
   */
  public Reader createReader(final WALIdentity path,
      CancelableProgressable reporter) throws IOException {
    WALProvider provider = getWALProvider();
    return provider.createReader(provider.createWALIdentity(path.toString()), reporter, true);
  }

  // These static methods are currently used where it's impractical to
  // untangle the reliance on state in the filesystem. They rely on singleton
  // WALFactory that just provides Reader / Writers.
  // For now, first Configuration object wins. Practically this just impacts the reader/writer class
  private static final AtomicReference<WALFactory> singleton = new AtomicReference<>();
  private static final String SINGLETON_ID = WALFactory.class.getName();
  
  // Public only for FSHLog
  public static WALFactory getInstance(Configuration configuration) {
    WALFactory factory = singleton.get();
    if (null == factory) {
      WALFactory temp = null;
      try {
        temp = new WALFactory(configuration);
      } catch (IOException ioe) {
        throw new IllegalStateException(ioe);
      }
      if (singleton.compareAndSet(null, temp)) {
        factory = temp;
      } else {
        // someone else beat us to initializing
        try {
          temp.close();
        } catch (IOException exception) {
          LOG.debug("failed to close temporary singleton. ignoring.", exception);
        }
        factory = singleton.get();
      }
    }
    return factory;
  }

  /**
   * Create a reader for the given path, accept custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * @return a WAL Reader, caller must close.
   */
  public static Reader createReader(final FileSystem fs, final Path path,
      final Configuration configuration) throws IOException {
    WALProvider provider = getInstance(configuration).getWALProvider();
    return provider.createReader(provider.createWALIdentity(path.toString()), null, true);
  }

  /**
   * Create a reader for the given path, accept custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * @return a WAL Reader, caller must close.
   */
  static Reader createReader(final FileSystem fs, final Path path,
      final Configuration configuration, final CancelableProgressable reporter) throws IOException {
    WALProvider provider = getInstance(configuration).getWALProvider();
    return provider.createReader(provider.createWALIdentity(path.toString()), reporter, true);
  }

  /**
   * Create a reader for the given path, ignore custom reader classes from conf.
   * If you already have a WALFactory, you should favor the instance method.
   * only public pending move of {@link org.apache.hadoop.hbase.regionserver.wal.Compressor}
   * @return a WAL Reader, caller must close.
   */
  public static Reader createReaderIgnoreCustomClass(final FileSystem fs, final Path path,
      final Configuration configuration) throws IOException {
    WALProvider provider = getInstance(configuration).getWALProvider();
    return provider.createReader(provider.createWALIdentity(path.toString()), null, false);
  }

  public final WALProvider getWALProvider() {
    return this.provider;
  }

  public final WALProvider getMetaWALProvider() {
    return this.metaProvider.get();
  }
}
