/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.StopStatus;
import org.apache.hadoop.hbase.master.RegionServerOperation.RegionServerOperationResult;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Keeps up the queue of {@link RegionServerOperation}s.
 * Has both live queue and a temporary put-aside queue; if processing of the
 * live todo queue fails for some reason, we'll add the item back on the delay
 * queue for retry later.  Call {@link #shutdown()} to effect a cleanup of
 * queues when done.  Listen to this queue by registering
 * {@link RegionServerOperationListener}s.
 * @see #registerRegionServerOperationListener(RegionServerOperationListener)
 * @see #unregisterRegionServerOperationListener(RegionServerOperationListener)
 */
public class RegionServerOperationQueue {
  // TODO: Build up the junit test of this class.
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final StopStatus stop;

  /**
   * Enums returned by {@link RegionServerOperationQueue#process()};
   */
  public static enum ProcessingResultCode {
    /**
     * Operation was processed successfully.
     */
    PROCESSED,
    /**
     * Nothing to do.
     */
    NOOP,
    /**
     * Operation was put-aside for now.  Will be retried later.
     */
    REQUEUED,
    /**
     * Failed processing of the operation.
     */
    FAILED,
    /**
     * Operation was requeued but we failed its processing for some reason
     * (Bad filesystem?).
     */
    REQUEUED_BUT_PROBLEM
  };

  /*
   * Do not put items directly on this queue. Use {@link #putOnDelayQueue(RegionServerOperation)}.
   * It makes sure the expiration on the RegionServerOperation added is updated.
   */
  private final DelayQueue<RegionServerOperation> delayedToDoQueue =
    new DelayQueue<RegionServerOperation>();
  private final BlockingQueue<RegionServerOperation> toDoQueue =
    new PriorityBlockingQueue<RegionServerOperation>();
  private final Set<RegionServerOperationListener> listeners =
    new CopyOnWriteArraySet<RegionServerOperationListener>();
  private final int threadWakeFrequency;
  private final Sleeper sleeper;
  private final ServerManager smgr;

  RegionServerOperationQueue(final Configuration c, ServerManager smgr,
      StopStatus stop) {
    this.threadWakeFrequency = c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000);
    this.smgr = smgr;
    this.stop = stop;
    this.sleeper = new Sleeper(this.threadWakeFrequency, stop);
  }

  public void put(final RegionServerOperation op) {
    try {
      this.toDoQueue.put(op);
    } catch (InterruptedException e) {
      LOG.warn("Insertion into todo queue interrupted; putting on delay queue", e);
      putOnDelayQueue(op);
    }
  }

  /**
   * Try to get an operation off of the queue and process it.
   * @return {@link ProcessingResultCode#PROCESSED},
   * {@link ProcessingResultCode#REQUEUED},
   * {@link ProcessingResultCode#REQUEUED_BUT_PROBLEM}
   */
  public synchronized ProcessingResultCode process() {
    RegionServerOperation op = null;
    // Only process the delayed queue if root region is online.  If offline,
    // the operation to put it online is probably in the toDoQueue.  Process
    // it first.
    //
    // The original logic used to starve the items in the delayed queue.
    //
    // Let us add all the ready items in the Delayed queue, behind the items in the
    // todo queue.
    while ((op = delayedToDoQueue.poll()) != null) {
      try {
        toDoQueue.put(op);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted", e);
      }
    }

    try {
      op = toDoQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted", e);
    }

    // At this point, if there's still no todo operation, or we're supposed to
    // be closed, return.
    if (op == null || stop.isStopped()) {
      return ProcessingResultCode.NOOP;
    }

    if (!(op instanceof ProcessServerShutdown)) {
      if (smgr.getServerInfo(op.serverName) == null) {
        LOG.debug("Discarding RegionServerOperation because server is dead " +
          op);
      }
    }
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Processing todo: " + op.toString());
      }
      if (!preProcess(op)) {
        // Add it back on the queue.
        putOnDelayQueue(op);
      } else {
        RegionServerOperationResult result = op.process();

        switch (result) {
        case OPERATION_SUCCEEDED:
          postProcess(op);
          return ProcessingResultCode.PROCESSED;

        case OPERATION_FAILED:
          putOnTodoQueue(op);
          break;
        case OPERATION_DELAYED:
          putOnDelayQueue(op);
          break;

        default:
          throw new RuntimeException("Invalid RegionServerProccessResult: "
              + result);
        }
      }
    } catch (Exception ex) {
      // There was an exception performing the operation.
      if (ex instanceof RemoteException) {
        try {
          ex = RemoteExceptionHandler.decodeRemoteException(
            (RemoteException)ex);
        } catch (IOException e) {
          ex = e;
          LOG.warn("main processing loop: " + op.toString(), e);
        }
      }

      LOG.warn("Failed processing: " + op.toString() +
        "; putting onto delayed todo queue", ex);
      putOnDelayQueue(op);
      return ProcessingResultCode.REQUEUED_BUT_PROBLEM;
    }
    return ProcessingResultCode.REQUEUED;
  }

  private void putOnDelayQueue(final RegionServerOperation op) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Put " + op.toString() + " back to the delay queue");
    }
    op.resetExpiration();
    this.delayedToDoQueue.put(op);
  }

  private void putOnTodoQueue(final RegionServerOperation op) {
    // Operation would have blocked because not all meta regions are
    // online. This could cause a deadlock, because this thread is waiting
    // for the missing meta region(s) to come back online, but since it
    // is waiting, it cannot process the meta region online operation it
    // is waiting for. So put this operation back on the queue for now.
    if (toDoQueue.size() == 0) {
      // The queue is currently empty so wait for a while to see if what
      // we need comes in first
      this.sleeper.sleep();
    }
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Put " + op.toString() + " back to the todo queue");
      }
      toDoQueue.put(op);
    } catch (InterruptedException e) {
      throw new RuntimeException("Putting into toDoQueue was interrupted.", e);
    }
  }

  /**
   * @return if the RSO queue has any entries
   */
  public boolean isEmpty() {
    return this.toDoQueue.isEmpty() && this.delayedToDoQueue.isEmpty();
  }

  /**
   * Clean up the queues.
   */
  public synchronized void shutdown() {
    this.toDoQueue.clear();
    this.delayedToDoQueue.clear();
  }

  /**
   * @param l Register this listener of RegionServerOperation events.
   */
  public void registerRegionServerOperationListener(final RegionServerOperationListener l) {
    this.listeners.add(l);
  }

  /**
   * @param l Unregister this listener for RegionServerOperation events.
   * @return True if this listener was registered.
   */
  public boolean unregisterRegionServerOperationListener(final RegionServerOperationListener l) {
    return this.listeners.remove(l);
  }

  /*
   * Tell listeners that we have processed a RegionServerOperation.
   *
   * @param op Operation to tell the world about.
   */
  private void postProcess(final RegionServerOperation op) {
    if (this.listeners.isEmpty()) return;
    for (RegionServerOperationListener listener: this.listeners) {
      listener.processed(op);
    }
  }

  /**
   * Called for each message passed the master.  Most of the messages that come
   * in here will go on to become {@link #preProcess(RegionServerOperation)}s but
   * others like {@linke HMsg.Type#MSG_REPORT_PROCESS_OPEN} go no further;
   * only in here can you see them come in.
   * @param serverInfo Server we got the message from.
   * @param incomingMsg The message received.
   * @return True to continue processing, false to skip.
   */
  boolean process(final HServerInfo serverInfo,
      final HMsg incomingMsg) {
    if (this.listeners.isEmpty()) return true;
    for (RegionServerOperationListener listener: this.listeners) {
      if (!listener.process(serverInfo, incomingMsg)) return false;
    }
    return true;
  }

  /*
   * Tell listeners that we start to process a RegionServerOperation.
   *
   * @param op Operation to tell the world about.
   */
  private boolean preProcess(final RegionServerOperation op) throws IOException {
    if (this.listeners.isEmpty()) return true;
    for (RegionServerOperationListener listener: this.listeners) {
      if (!listener.process(op)) return false;
    }
    return true;
  }
}
