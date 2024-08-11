// Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// This software, the RabbitMQ Java client library, is triple-licensed under the
// Mozilla Public License 2.0 ("MPL"), the GNU General Public License version 2
// ("GPL") and the Apache License version 2 ("ASL"). For the MPL, please see
// LICENSE-MPL-RabbitMQ. For the GPL, please see LICENSE-GPL2.  For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.


package com.rabbitmq.utility;

import java.util.concurrent.TimeoutException;

/**
 * Simple one-shot IPC mechanism. Essentially a one-place buffer that cannot be emptied once filled.
 * 简单的一次性IPC机制。本质上是一个单位缓冲区，一旦填满就不能清空
 */
public class BlockingCell<T> {
    /** Indicator of not-yet-filledness */
    private boolean _filled = false; // 用来表示_value是否被设置了

    /** Will be null until a value is supplied, and possibly still then. */
    private T _value;

    private static final long NANOS_IN_MILLI = 1000L * 1000L;

    private static final long INFINITY = -1;

    /** Instantiate a new BlockingCell waiting for a value of the specified type. */
    public BlockingCell() {
        // no special handling required in default constructor
    }

    /**
     * Wait for a value, and when one arrives, return it (without clearing it). If there's already a value present, there's no need to wait - the existing value
     * is returned.
     * @return the waited-for value
     *
     * @throws InterruptedException if this thread is interrupted
     */
    public synchronized T get() throws InterruptedException {
        while (!_filled) {
            wait();
        }
        return _value;
    }

    /**
     * Wait for a value, and when one arrives, return it (without clearing it). If there's
     * already a value present, there's no need to wait - the existing value is returned.
     * If timeout is reached and value hasn't arrived, TimeoutException is thrown.
     * 等待一个值，当值到达时，返回它（而不清除它）。如果已经存在一个值，则不需要等待——直接返回已有的值。如果超时时间到了，且值还没有到达，则抛出 TimeoutException 异常。
     * @param timeout timeout in milliseconds. -1 effectively means infinity
     * @return the waited-for value
     * @throws InterruptedException if this thread is interrupted
     */
    public synchronized T get(long timeout) throws InterruptedException, TimeoutException {
        if (timeout == INFINITY) return get();

        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout cannot be less than zero");
        }

        long now = System.nanoTime() / NANOS_IN_MILLI;
        long maxTime = now + timeout;
        while (!_filled && (now = (System.nanoTime() / NANOS_IN_MILLI)) < maxTime) {
            wait(maxTime - now);
        }

        if (!_filled)
            throw new TimeoutException();

        return _value;
    }

    /**
     * As get(), but catches and ignores InterruptedException, retrying until a value appears.
     * @return the waited-for value
     */
    public synchronized T uninterruptibleGet() {
        boolean wasInterrupted = false;
        try {
            while (true) {
                try {
                    return get();
                } catch (InterruptedException ex) {
                    // no special handling necessary
                    wasInterrupted = true;
                }
            }
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * As get(long timeout), but catches and ignores InterruptedException, retrying until
     * a value appears or until specified timeout is reached. If timeout is reached,
     * TimeoutException is thrown.
     * We also use System.nanoTime() to behave correctly when system clock jumps around.
     * // 与 get(long timeout) 类似，但会捕获并忽略 InterruptedException，重试直到值出现或达到指定的超时时间。如果超时，抛出 TimeoutException 异常。我们还使用 System.nanoTime() 来确保在系统时钟发生跳变时能够正确运行
     * @param timeout timeout in milliseconds. -1 means 'infinity': never time out
     * @return the waited-for value
     */
    public synchronized T uninterruptibleGet(int timeout) throws TimeoutException {
        long now = System.nanoTime() / NANOS_IN_MILLI;
        long runTime = now + timeout;
        boolean wasInterrupted = false;
        try {
            do {
                try {
                    return get(runTime - now);
                } catch (InterruptedException e) {
                    // Ignore.
                    wasInterrupted = true; // 被中断会一直重试，直到超时或者成功
                }
            } while ((timeout == INFINITY) || ((now = System.nanoTime() / NANOS_IN_MILLI) < runTime));
        } finally {
            if (wasInterrupted) {
                Thread.currentThread().interrupt(); // 被中断，最后还是超时，需要中断当前等待线程
            }
        }

        throw new TimeoutException();
    }

    /**
     * Store a value in this BlockingCell, throwing {@link IllegalStateException} if the cell already has a value.
     * @param newValue the new value to store
     */
    public synchronized void set(T newValue) {
        if (_filled) {
            throw new IllegalStateException("BlockingCell can only be set once");
        }
        _value = newValue; // 举个例子：ValueOrException={#method<connection.start>(version-major=0, version-minor=9, server-properties={cluster_name=rabbit@WIN-20230608VMY, copyright=Copyright (c) 2007-2024 Broadcom Inc and/or its subsidiaries, product=RabbitMQ, capabilities={consumer_priorities=true, exchange_exchange_bindings=true, connection.blocked=true, authentication_failure_close=true, per_consumer_qos=true, basic.nack=true, direct_reply_to=true, publisher_confirms=true, consumer_cancel_notify=true}, information=Licensed under the MPL 2.0. Website: https://rabbitmq.com, version=3.13.2, platform=Erlang/OTP 27.0}, mechanisms=PLAIN AMQPLAIN, locales=en_US), null, ""}
        _filled = true;
        notifyAll();
    }

    /**
     * Store a value in this BlockingCell if it doesn't already have a value.
     * @return true if this call to setIfUnset actually updated the BlockingCell; false if the cell already had a value.
     * @param newValue the new value to store
     */
    public synchronized boolean setIfUnset(T newValue) {
        if (_filled) {
            return false;
        }
        set(newValue);
        return true;
    }
}
