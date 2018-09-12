package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.wal.WALIdentity;

import java.io.IOException;

import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Streaming access to WAL entries. This class is given a queue of WAL {@link Path}, and continually
 * iterates through all the WAL {@link Entry} in the queue. When it's done reading from a Path, it
 * dequeues it and starts reading from the next.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WALEntryStream extends WAL.Reader {
  /**
   * @return the {@link WALIdentity} of the current WAL
   */
  public WALIdentity getCurrentWALIdentity();

  /**
   * @return true if there is another WAL {@link Entry}
   */
  public boolean hasNext() throws IOException;

  /**
   * Returns the next WAL entry in this stream but does not advance.
   */
  public Entry peek() throws IOException;
}
