/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2012 Couchbase, Inc.
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package net.spy.memcached.protocol;

import net.spy.memcached.MemcachedNode;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.ops.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class for protocol-specific operation implementations.
 */
public abstract class BaseOperationImpl extends SpyObject implements Operation {

    /**
     * Status object for canceled operations.
     */
    public static final OperationStatus CANCELLED =
            new CancelledOperationStatus();
    public static final OperationStatus TIMED_OUT =
            new TimedOutOperationStatus();
    protected OperationCallback callback = null;
    protected Collection<MemcachedNode> notMyVbucketNodes =
            new HashSet<MemcachedNode>();
    ReentrantLock lock = new ReentrantLock();
    private volatile OperationState state = OperationState.WRITE_QUEUED;
    private ByteBuffer cmd = null;
    private boolean cancelled = false;
    private OperationException exception = null;
    private volatile MemcachedNode handlingNode = null;
    private volatile boolean timedout;
    private long creationTime;
    private boolean timedOutUnsent = false;
    private long writeCompleteTimestamp;
    /**
     * If the operation gets cloned, the reference is used to cascade cancellations
     * and timeouts.
     */
    private Clones clones = Clones.of(Collections.synchronizedList(new ArrayList<Operation>()));
    /**
     * Number of clones for this operation.
     */
    private volatile int cloneCount;

    public BaseOperationImpl() {
        super();
        creationTime = System.nanoTime();
    }

    /**
     * Get the operation callback associated with this operation.
     */
    public final OperationCallback getCallback() {
        return callback;
    }

    /**
     * Set the callback for this instance.
     */
    protected void setCallback(OperationCallback to) {
        callback = to;
    }

    public final boolean isCancelled() {
        try {
            clones.lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            return cancelled;
        } finally {
            clones.lock.unlock();
        }
    }

    public final boolean hasErrored() {
        return exception != null;
    }

    public final OperationException getException() {
        return exception;
    }

    public final void cancel() {

        try {
            clones.lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        cancelled = true;
        try {
            Iterator<Operation> i = clones.clones.iterator();
            while (i.hasNext()) {
                i.next().cancel();
            }
        } finally {
            clones.lock.unlock();
        }


        wasCancelled();
        callback.receivedStatus(CANCELLED);
        callback.complete();
    }

    /**
     * This is called on each subclass whenever an operation was cancelled.
     */
    protected void wasCancelled() {
        getLogger().debug("was cancelled.");
    }

    public final OperationState getState() {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    public final ByteBuffer getBuffer() {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            return cmd;
        } finally {
            lock.unlock();
        }

    }

    /**
     * Set the write buffer for this operation.
     */
    protected final void setBuffer(ByteBuffer to) {
        assert to != null : "Trying to set buffer to null";
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            cmd = to;
            cmd.mark();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Transition the state of this operation to the given state.
     */
    protected final void transitionState(OperationState newState) {
        getLogger().debug("Transitioned state from %s to %s", state, newState);
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            state = newState;
            // Discard our buffer when we no longer need it.
            if (state != OperationState.WRITE_QUEUED
                    && state != OperationState.WRITING) {
                cmd = null;
            }
            if (state == OperationState.COMPLETE) {
                callback.complete();
            }
        } finally {
            lock.unlock();
        }
    }

    public final void writing() {
        transitionState(OperationState.WRITING);
    }

    public final void writeComplete() {
        writeCompleteTimestamp = System.nanoTime();
        transitionState(OperationState.READING);
    }

    public abstract void initialize();

    public abstract void readFromBuffer(ByteBuffer data) throws IOException;

    protected void handleError(OperationErrorType eType, String line)
            throws IOException {
        getLogger().error("Error:  %s", line);
        switch (eType) {
            case GENERAL:
                exception = new OperationException();
                break;
            case SERVER:
                exception = new OperationException(eType, line);
                break;
            case CLIENT:
                exception = new OperationException(eType, line);
                break;
            default:
                assert false;
        }
        callback.receivedStatus(new OperationStatus(false,
                exception.getMessage(), StatusCode.ERR_INTERNAL));
        transitionState(OperationState.COMPLETE);
        throw exception;
    }

    public void handleRead(ByteBuffer data) {
        assert false;
    }

    public MemcachedNode getHandlingNode() {
        return handlingNode;
    }

    public void setHandlingNode(MemcachedNode to) {
        handlingNode = to;
    }

    @Override
    public void timeOut() {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            timedout = true;

            try {
                clones.lock.lockInterruptibly();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                Iterator<Operation> i = clones.clones.iterator();
                while (i.hasNext()) {
                    i.next().timeOut();
                }
            } finally {
                clones.lock.unlock();
            }

            callback.receivedStatus(TIMED_OUT);
            callback.complete();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isTimedOut() {
      try {
        lock.lockInterruptibly();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      try {
        return timedout;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean isTimedOut(long ttlMillis) {
      try {
        lock.lockInterruptibly();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      try {
        long elapsed = System.nanoTime();
        long ttlNanos = ttlMillis * 1000 * 1000;
        if (elapsed - creationTime > ttlNanos) {
          timedOutUnsent = true;
          timedout = true;
          callback.receivedStatus(TIMED_OUT);
          callback.complete();
        } // else
        // timedout would be false, but we cannot allow you to untimeout an
        // operation.  This can happen when the latch timeout is shorter than the
        // default operation timeout.
        return timedout;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean isTimedOutUnsent() {
        return timedOutUnsent;
    }

    @Override
    public long getWriteCompleteTimestamp() {
        return writeCompleteTimestamp;
    }

    @Override
    public void addClone(Operation op) {
        clones.clones.add(op);
    }

    @Override
    public int getCloneCount() {
        return cloneCount;
    }

    @Override
    public void setCloneCount(int count) {
        cloneCount = count;
    }

    static class Clones {
        List<Operation> clones;
        ReentrantLock lock;

        public Clones(List<Operation> clones) {
            this.clones = clones;
            this.lock = new ReentrantLock();
        }

        public static Clones of(List<Operation> clones) {
            return new Clones(clones);
        }
    }
}
