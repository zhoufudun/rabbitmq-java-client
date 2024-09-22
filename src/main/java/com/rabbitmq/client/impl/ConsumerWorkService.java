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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个客户端连接（AMQConnection）一个实例
 * 一个ConsumerWorkService 对应一个workPool
 */
final public class ConsumerWorkService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerWorkService.class);
    private static final int MAX_RUNNABLE_BLOCK_SIZE = 256;
    private static final int DEFAULT_NUM_THREADS = Math.max(1, Utils.availableProcessors());
    private final ExecutorService executor;
    private final boolean privateExecutor;
    private final WorkPool<Channel, Runnable> workPool; // 一个客户端连接（AMQConnection）对应一个WorkPool
    private final int shutdownTimeout;

    public ConsumerWorkService(ExecutorService executor, ThreadFactory threadFactory, int queueingTimeout, int shutdownTimeout) {
        this.privateExecutor = (executor == null);
        if (executor == null) {
            LOGGER.debug("Creating executor service with {} thread(s) for consumer work service", DEFAULT_NUM_THREADS);
            this.executor = Executors.newFixedThreadPool(DEFAULT_NUM_THREADS, threadFactory);
        } else {
            this.executor = executor;
        }
        this.workPool = new WorkPool<>(queueingTimeout);
        this.shutdownTimeout = shutdownTimeout;
    }

    public ConsumerWorkService(ExecutorService executor, ThreadFactory threadFactory, int shutdownTimeout) {
        this(executor, threadFactory, -1, shutdownTimeout);
    }

    public int getShutdownTimeout() {
        return shutdownTimeout;
    }

    /**
     * Stop executing all consumer work
     */
    public void shutdown() {
        this.workPool.unregisterAllKeys();
        if (privateExecutor)
            this.executor.shutdown();
    }

    /**
     * Stop executing all consumer work for a particular channel
     * @param channel to stop consumer work for
     */
    public void stopWork(Channel channel) {
        this.workPool.unregisterKey(channel);
    }

    public void registerKey(Channel channel) {
        this.workPool.registerKey(channel);
    }

    public void setUnlimited(Channel channel, boolean unlimited) {
        if (unlimited) {
            this.workPool.unlimit(channel);
        } else {
            this.workPool.limit(channel);
        }
    }

    public void addWork(Channel channel, Runnable runnable) {
        if (this.workPool.addWorkItem(channel, runnable)) { // 将客户端的Runnable加入绑定的唯一队列，在启动一个线程统一消费
            this.executor.execute(new WorkPoolRunnable()); // 提交一个任务，他可以处理一批客户端的消息
        }
    }

    /**
     * @return true if executor used by this work service is managed
     *              by it and wasn't provided by the user
     */
    public boolean usesPrivateExecutor() {
        return privateExecutor;
    }

    // 运行一批客户端的任务后退出
    private final class WorkPoolRunnable implements Runnable {

        @Override
        public void run() {
            int size = MAX_RUNNABLE_BLOCK_SIZE; // 一次性最大处理256个
            List<Runnable> block = new ArrayList<Runnable>(size);
            try {
                Channel key = ConsumerWorkService.this.workPool.nextWorkBlock(block, size); // 获取一个客户端的全部消息（List<Runnable>）
                if (key == null) return; // nothing ready to run
                try {
                    for (Runnable runnable : block) {
                        runnable.run();
                    }
                } finally {
                    if (ConsumerWorkService.this.workPool.finishWorkBlock(key)) {
                        // 如果客户端有更多的工作项，继续提交任务执行
                        ConsumerWorkService.this.executor.execute(new WorkPoolRunnable());
                    }
                }
            } catch (RuntimeException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
