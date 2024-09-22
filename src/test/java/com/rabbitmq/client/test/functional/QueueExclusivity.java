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


package com.rabbitmq.client.test.functional;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.test.BrokerTestCase;

/**
 * read
 */
// Test queue auto-delete and exclusive semantics.
public class QueueExclusivity extends BrokerTestCase {

    final HashMap<String, Object> noArgs = new HashMap<String, Object>();

    public Connection altConnection;
    public Channel altChannel;
    final String q = "exclusiveQ";

    protected void createResources() throws IOException, TimeoutException {
        altConnection = connectionFactory.newConnection();
        altChannel = altConnection.createChannel();
        altChannel.queueDeclare(q,
                // not durable, exclusive, not auto-delete
                // 非持久化，独占的（不同channel之前创建的队列无法被互相修改），非自动删除
                false, true, false, noArgs);
    }

    protected void releaseResources() throws IOException {
        if (altConnection != null && altConnection.isOpen()) {
            altConnection.close();
        }
    }

    @Test
    public void queueExclusiveForPassiveDeclare() throws Exception {
        try {
            // 被动申明，只检查队列存不存在，不进行创建或修改操作
            channel.queueDeclarePassive(q);
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        /**
         * “当从另一个连接对排他性队列进行被动声明时，该操作应该失败。”
         * 在AMQP协议中，队列的排他性意味着只有创建该队列的连接才能使用它。
         * 被动声明队列是指客户端仅检查队列是否存在，而不进行任何创建或修改操作。
         * 如果从另一个连接尝试对一个排他性队列进行被动声明，那么根据排他性的定义，
         * 这个操作应该会失败，并可能抛出一个AMQP资源锁定（RESOURCE_LOCKED）的错误
         */
        fail("Passive queue declaration of an exclusive queue from another connection should fail");
    }

    // This is a different scenario because active declare takes notice of
    // the all the arguments

    /**
     * 使用channel.queueDeclare方法进行队列声明时，如果是主动声明（即带有参数的声明），那么该方法会注意到所有的参数，
     * 但是如果队列已经被声明为排他性的，那么该方法会抛出一个AMQP资源锁定（RESOURCE_LOCKED）的错误。
     */
    @Test
    public void queueExclusiveForDeclare() throws Exception {
        try {
            channel.queueDeclare(q, false, true, false, noArgs);
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        fail("Active queue declaration of an exclusive queue from another connection should fail");
    }

    @Test
    public void queueExclusiveForConsume() throws Exception {
        QueueingConsumer c = new QueueingConsumer(channel);
        try {
            channel.basicConsume(q, c);
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        /**
         * 排他队列无法被其他的channel上的消费者消费
         */
        fail("Exclusive queue should be locked for basic consume from another connection");
    }

    @Test
    public void queueExclusiveForPurge() throws Exception {
        try {
            channel.queuePurge(q);
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        // 排他队列无法被其他channel执行Purge操作
        fail("Exclusive queue should be locked for queue purge from another connection");
    }

    @Test
    public void queueExclusiveForDelete() throws Exception {
        try {
            channel.queueDelete(q);
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        // 排他队列无法被其他channel执行delete操作
        fail("Exclusive queue should be locked for queue delete from another connection");
    }

    @Test
    public void queueExclusiveForBind() throws Exception {
        try {
            // 排他队列无法被其他channel执行bind操作
            channel.queueBind(q, "amq.direct", "");
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        fail("Exclusive queue should be locked for queue bind from another connection");
    }

    // NB The spec XML doesn't mention queue.unbind, basic.cancel, or
    // basic.get in the exclusive rule. It seems the most sensible
    // interpretation to include queue.unbind and basic.get in the
    // prohibition.
    // basic.cancel is inherently local to a channel, so it
    // *doesn't* make sense to include it.

    @Test
    public void queueExclusiveForUnbind() throws Exception {
        // 排他队列无法被其他channel执行bind操作，可以被自己的channel执行bind操作
        altChannel.queueBind(q, "amq.direct", "");
        try {
            // 排他队列无法被其他channel执行bind操作
            channel.queueUnbind(q, "amq.direct", "");
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        fail("Exclusive queue should be locked for queue unbind from another connection");
    }

    @Test
    public void queueExclusiveForGet() throws Exception {
        try {
            // 排他队列无法被其他channel执行主动get操作
            channel.basicGet(q, true);
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.RESOURCE_LOCKED, ioe);
            return;
        }
        fail("Exclusive queue should be locked for basic get from another connection");
    }

}
