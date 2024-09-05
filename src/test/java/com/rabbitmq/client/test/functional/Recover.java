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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.test.BrokerTestCase;

public class Recover extends BrokerTestCase {

    String queue;
    final byte[] body = "message".getBytes();

    public void createResources() throws IOException {
        AMQP.Queue.DeclareOk ok = channel.queueDeclare();
        queue = ok.getQueue();
    }

    static interface RecoverCallback {
        void recover(Channel channel) throws IOException;
    }

    // The AMQP specification under-specifies the behaviour when
    // requeue=false.  So we can't really test any scenarios for
    // requeue=false.

    void verifyRedeliverOnRecover(RecoverCallback call) throws IOException, InterruptedException {
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, false, consumer); // require acks.
        channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().build(), body);
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        assertTrue(Arrays.equals(body, delivery.getBody()), "consumed message body not as sent");
        // Don't ack it, and get it redelivered to the same consumer
        call.recover(channel);
        QueueingConsumer.Delivery secondDelivery = consumer.nextDelivery(5000);
        assertNotNull(secondDelivery, "timed out waiting for redelivered message");
        assertTrue(Arrays.equals(body, delivery.getBody()), "consumed (redelivered) message body not as sent");
    }

    void verifyNoRedeliveryWithAutoAck(RecoverCallback call) throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<byte[]> bodyReference = new AtomicReference<byte[]>();
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                bodyReference.set(body);
                latch.countDown();
            }
        };
        /**
         * 收到消息后，会调用consumer的handleDelivery方法，handleDelivery方法会将消息的body赋值给bodyReference，
         * 然后调用latch.countDown()方法，将latch计数器减1，这样就可以保证在handleDelivery方法执行完后，latch计数器的值为0。
         * 同时自动回复ack，告诉服务端消费成功
         */
        channel.basicConsume(queue, false, consumer); // auto ack.
        channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().build(), body);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(Arrays.equals(body, bodyReference.get()), "consumed message body not as sent");
        // Don't ack it, and get it redelivered to the same consumer 作用是重新发送未被确认的消息到消费者
        call.recover(channel);
        // 主动获取发现没有消息，所以返回null
        assertNotNull(channel.basicGet(queue, true), "should be no message available");
    }

    final RecoverCallback recoverSync = new RecoverCallback() {
        public void recover(Channel channel) throws IOException {
            /**
             * channel.basicRecover(true); 的作用是重新发送未被确认的消息到消费者。
             * 在AMQP（Advanced Message Queuing Protocol）中，消息的确认机制是确保消息被正确处理的重要环节。
             * 当消费者接收到一条消息时，它可以选择确认（acknowledge）这条消息，表示它已经成功处理了该消息，
             * 或者选择不确认（nack）这条消息，表示它无法处理该消息或者处理过程中出现了错误。
             * 当设置为true时，调用basicRecover方法会告诉RabbitMQ服务器将所有未被确认的消息重新发送给消费者。
             * 这个操作通常在消费者由于某些原因（例如网络问题、消费者崩溃等）未能及时确认消息时使用，以确保消息不会丢失
             */
            channel.basicRecover(true);
        }
    };

    final RecoverCallback recoverSyncConvenience = new RecoverCallback() {
        public void recover(Channel channel) throws IOException {
            channel.basicRecover();
        }
    };

    @Test
    public void redeliveryOnRecover() throws IOException, InterruptedException {
        verifyRedeliverOnRecover(recoverSync);
    }

    @Test
    public void redeliverOnRecoverConvenience() throws IOException, InterruptedException {
        verifyRedeliverOnRecover(recoverSyncConvenience);
    }

    @Test
    public void noRedeliveryWithAutoAck() throws IOException, InterruptedException {
        verifyNoRedeliveryWithAutoAck(recoverSync);
    }

    @Test
    public void requeueFalseNotSupported() throws Exception {
        try {
            channel.basicRecover(false);
            fail("basicRecover(false) should not be supported");
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.NOT_IMPLEMENTED, ioe);
        }
    }
}
