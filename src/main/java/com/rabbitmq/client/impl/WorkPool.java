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

package com.rabbitmq.client.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * <p>This is a generic implementation of the channels specification
 * in <i>Channeling Work</i>, Nov 2010 (<tt>channels.pdf</tt>).
 * Objects of type <b>K</b> must be registered, with <code><b>registerKey(K)</b></code>,
 * and then they become <i>clients</i> and a queue of
 * items (type <b>W</b>) is stored for each client.
 * </p>
 * Each client has a <i>state</i> which is exactly one of <i>dormant</i>,
 * <i>in progress</i> or <i>ready</i>. Immediately after registration a client is <i>dormant</i>.
 * Items may be (singly) added to (the end of) a client's queue with {@link WorkPool#addWorkItem(Object, Object)}.
 * If the client is <i>dormant</i> it becomes <i>ready</i> thereby. All other states remain unchanged.
 * The next <i>ready</i> client, together with a collection of its items,
 * may be retrieved with <code><b>nextWorkBlock(collection,max)</b></code>
 * (making that client <i>in progress</i>).
 * An <i>in progress</i> client can finish (processing a batch of items) with <code><b>finishWorkBlock(K)</b></code>.
 * It then becomes either <i>dormant</i> or <i>ready</i>, depending if its queue of work items is empty or no.
 * If a client has items queued, it is either <i>in progress</i> or <i>ready</i> but cannot be both.
 * When work is finished it may be marked <i>ready</i> if there is further work,
 * or <i>dormant</i> if there is not.
 * There is never any work for a <i>dormant</i> client.
 * A client may be unregistered, with <code><b>unregisterKey(K)</b></code>, which removes the client from
 * all parts of the state, and any queue of items stored with it.
 * All clients may be unregistered with <code><b>unregisterAllKeys()</b></code>.
 * <h2>Concurrent Semantics</h2>
 * This implementation is thread-safe.
 *
 * @param <K> Key -- type of client
 * @param <W> Work -- type of work item
 *            <p>
 *            <p>
 *            必须使用registerKey（K）注册K类型的对象，
 *            然后它们成为客户端，并为每个客户端存储一个项目队列（W类型）。
 *            每个客户端都有一个状态，该状态恰好是休眠、进行中或就绪状态之一。
 *            注册后，客户立即处于休眠状态。可以使用addWorkItem（Object，Object）将项目（单独）添加到客户端的队列（末尾）。
 *            如果客户端处于休眠状态，它就会准备就绪。所有其他州保持不变。
 *            下一个就绪的客户端及其项的集合可以使用nextWorkBlock（collection，max）检索（使该客户端正在进行中）。
 *            正在进行的客户端可以使用finishWorkBlock（K）完成（处理一批项目）。然后，它要么处于休眠状态，要么处于就绪状态，
 *            这取决于它的工作项队列是空的还是否。如果客户端有排队的项目，它要么正在进行中，要么已经就绪，但不能两者兼而有之。
 *            工作完成后，如果有进一步的工作，它可能会被标记为准备就绪，如果没有，则标记为休眠。
 *            对于一个不活跃的客户来说，从来没有任何工作。客户端可以使用unregisterKey（K）进行注销，
 *            这会将客户端从状态的所有部分以及与之存储的任何项目队列中删除。所有客户端都可以使用unregistrAllKey（）进行注销。
 *
 *
 *           一个客户端连接（AMQConnection）对应一个WorkPool
 */
public class WorkPool<K, W> {
    private static final int MAX_QUEUE_LENGTH = 1000;

    /**
     * An injective queue of <i>ready</i> clients.
     */
    private final SetQueue<K> ready = new SetQueue<K>(); // 准备处理的客户端集合
    /**
     * The set of clients which have work <i>in progress</i>.
     */ // 正在处理中的客户端集合
    private final Set<K> inProgress = new HashSet<K>();
    /**
     * The pool of registered clients, with their work queues.
     */     // 每一个channel拥有一个队列，同一个AMQConnection上会有多个channel
    private final Map<K, VariableLinkedBlockingQueue<W>> pool = new HashMap<>();
    /**
     * Those keys which want limits to be removed. We do not limit queue size if this is non-empty.
     */
    // 那些希望移除限制的键。如果这个集合非空，我们将不限制队列大小
    private final Set<K> unlimited = new HashSet<K>();
    private final BiConsumer<VariableLinkedBlockingQueue<W>, W> enqueueingCallback;

    public WorkPool(final int queueingTimeout) {
        if (queueingTimeout > 0) {
            this.enqueueingCallback = (queue, item) -> {
                try {
                    boolean offered = queue.offer(item, queueingTimeout, TimeUnit.MILLISECONDS);
                    if (!offered) {
                        throw new WorkPoolFullException("Could not enqueue in work pool after " + queueingTimeout + " ms.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
        } else {
            this.enqueueingCallback = (queue, item) -> {
                try {
                    queue.put(item);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
        }
    }

    /**
     * Add client <code><b>key</b></code> to pool of item queues, with an empty queue.
     * A client is initially <i>dormant</i>.
     * No-op if <code><b>key</b></code> already present.  将客户端key添加到项队列池中，并初始化为空队列。客户端初始状态为休眠。如果key已经存在，则不进行操作
     *
     * @param key client to add to pool
     */
    public void registerKey(K key) {
        synchronized (this) {
            if (!this.pool.containsKey(key)) {
                int initialCapacity = unlimited.isEmpty() ? MAX_QUEUE_LENGTH : Integer.MAX_VALUE;
                this.pool.put(key, new VariableLinkedBlockingQueue<W>(initialCapacity)); // 每个AMQChannel对应一个VariableLinkedBlockingQueue
            }
        }
    }

    public synchronized void limit(K key) {
        unlimited.remove(key);
        if (unlimited.isEmpty()) { // channel对应的queue不限制，设置最大值=1000大小
            setCapacities(MAX_QUEUE_LENGTH);
        }
    }

    public synchronized void unlimit(K key) {
        unlimited.add(key);
        if (!unlimited.isEmpty()) { // channel对应的queue不受限制，设置最大值=2^31-1
            setCapacities(Integer.MAX_VALUE);
        }
    }

    private void setCapacities(int capacity) {
        Iterator<VariableLinkedBlockingQueue<W>> it = pool.values().iterator();
        while (it.hasNext()) {
            it.next().setCapacity(capacity);
        }
    }

    /**
     * Remove client from pool and from any other state. Has no effect if client already absent.
     *
     * @param key of client to unregister
     */
    public void unregisterKey(K key) {
        synchronized (this) {
            this.pool.remove(key);
            this.ready.remove(key);
            this.inProgress.remove(key);
            this.unlimited.remove(key);
        }
    }

    /**
     * Remove all clients from pool and from any other state.
     */
    public void unregisterAllKeys() {
        synchronized (this) {
            this.pool.clear();
            this.ready.clear();
            this.inProgress.clear();
            this.unlimited.clear();
        }
    }

    /**
     * Return the next <i>ready</i> client,
     * and transfer a collection of that client's items to process.
     * Mark client <i>in progress</i>.
     * If there is no <i>ready</i> client, return <code><b>null</b></code>.
     * 从休眠队列key（channel），并且将key存入表示处理中的队列里，在将该key（channel）的所有任务都移到集合中，外部会处理这里任务
     * @param to   collection object in which to transfer items
     * @param size max number of items to transfer
     * @return key of client to whom items belong, or <code><b>null</b></code> if there is none.
     */
    public K nextWorkBlock(Collection<W> to, int size) {
        synchronized (this) {
            K nextKey = readyToInProgress(); // 将read状态的channel转变为处理中状态： ready集合中移除，加入InProgress集合。。。。 key 举例：RecoveryAwareChannelN = AMQChannel(amqp://guest@127.0.0.1:5672//zfdtest,1)
            if (nextKey != null) {
                VariableLinkedBlockingQueue<W> queue = this.pool.get(nextKey);
                drainTo(queue, to, size); // queue消息全部倒入to集合中
            }
            return nextKey;
        }
    }

    /**
     * Private implementation of <code><b>drainTo</b></code> (not implemented for <code><b>LinkedList&lt;W&gt;</b></code>s).
     *
     * @param deList      to take (poll) elements from
     * @param c           to add elements to
     * @param maxElements to take from deList
     * @return number of elements actually taken
     */
    private int drainTo(VariableLinkedBlockingQueue<W> deList, Collection<W> c, int maxElements) {
        int n = 0;
        while (n < maxElements) {
            W first = deList.poll();
            if (first == null)
                break;
            c.add(first);
            ++n;
        }
        return n;
    }

    /**
     * 为特定客户端添加（入队）一个item。如果客户端未注册，则不进行更改并返回 false。如果客户端处于休眠状态，则将其标记为就绪。
     * Add (enqueue) an item for a specific client.
     * No change and returns <code><b>false</b></code> if client not registered.
     * If <i>dormant</i>, the client will be marked <i>ready</i>.
     *
     * @param key  the client to add to the work item to
     * @param item the work item to add to the client queue
     * @return <code><b>true</b></code> if and only if the client is marked <i>ready</i>
     * &mdash; <i>as a result of this work item</i>
     */
    public boolean addWorkItem(K key, W item) {
        VariableLinkedBlockingQueue<W> queue;
        synchronized (this) {
            queue = this.pool.get(key);
        }
        // The put operation may block. We need to make sure we are not holding the lock while that happens.
        if (queue != null) {
            enqueueingCallback.accept(queue, item); // item是一个Runnable或者其他类型

            synchronized (this) {
                // 客户端处于睡眠中
                if (isDormant(key)) { // 客户端已经注册 && 没有处理中 && 没有准备好
                    dormantToReady(key); // 将客户端标记为准备：休眠状态转变为准备状态
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 标记完成工作的客户端不再处于进行中状态。如果客户端未注册或未处于进行中状态，将抛出异常。如果客户端有更多的工作项，它将被标记为就绪；否则，它将被标记为休眠
     * Set client no longer <i>in progress</i>.
     * Ignore unknown clients (and return <code><b>false</b></code>).
     *
     * @param key client that has finished work
     * @return <code><b>true</b></code> if and only if client becomes <i>ready</i>
     * @throws IllegalStateException if registered client not <i>in progress</i>
     *                               <p>
     *                               返回值表示当前客户端的任务是都已经全部处理完毕，如果还有任务返回true
     */
    public boolean finishWorkBlock(K key) {
        synchronized (this) {
            if (!this.isRegistered(key))
                return false;
            if (!this.inProgress.contains(key)) {
                throw new IllegalStateException("Client " + key + " not in progress");
            }

            if (moreWorkItems(key)) {
                inProgressToReady(key);
                return true; // 当前channel还有数据，返回true，外部继续提交任务处理
            } else {
                inProgressToDormant(key);
                return false;
            }
        }
    }

    private boolean moreWorkItems(K key) {
        VariableLinkedBlockingQueue<W> leList = this.pool.get(key);
        return leList != null && !leList.isEmpty();
    }

    /* State identification functions */
    private boolean isInProgress(K key) {
        return this.inProgress.contains(key);
    }

    private boolean isReady(K key) {
        return this.ready.contains(key);
    }

    private boolean isRegistered(K key) {
        return this.pool.containsKey(key);
    }

    private boolean isDormant(K key) {
        return !isInProgress(key) && !isReady(key) && isRegistered(key);
    }

    /* State transition methods - all assume key registered */

    /**
     * 首先，从 "inProgress" 集合中移除指定的客户端键值 "key"。这一步表示客户端不再处于 "in progress" 状态。
     * 然后，尝试将该客户端键值添加到 "ready" 集合中，但是只有在 "ready" 集合中不存在该键值时才会添加。这一步表示将客户端标记为 "ready"
     *
     * @param key
     */
    private void inProgressToReady(K key) {
        this.inProgress.remove(key);
        this.ready.addIfNotPresent(key);
    }

    private void inProgressToDormant(K key) {
        this.inProgress.remove(key);
    }

    private void dormantToReady(K key) {
        this.ready.addIfNotPresent(key);
    }

    /* Basic work selector and state transition step */
    private K readyToInProgress() {
        K key = this.ready.poll();
        if (key != null) {
            this.inProgress.add(key);
        }
        return key;
    }

}
