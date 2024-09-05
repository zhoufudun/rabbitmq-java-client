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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.test.TestUtils;
import com.rabbitmq.client.test.TestUtils.CallableFunction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import java.util.UUID;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * read
 */
public class Nack extends AbstractRejectTest {

    public static Object[] queueCreators() {
        return new Object[]{
                (CallableFunction<Channel, String>) channel -> {
                    String q = UUID.randomUUID().toString();
                    channel.queueDeclare(q, true, false, false, Collections.singletonMap("x-queue-type", "quorum"));
                    return q;
                },
                (CallableFunction<Channel, String>) channel -> {
                    String q = UUID.randomUUID().toString();
                    channel.queueDeclare(q, true, false, false, Collections.singletonMap("x-queue-type", "classic"));
                    return q;
                }};
    }

    @ParameterizedTest
    @MethodSource("queueCreators")
    public void singleNack(TestUtils.CallableFunction<Channel, String> queueCreator) throws Exception {
        String q = queueCreator.apply(channel);

        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();

        channel.confirmSelect();

        basicPublishVolatile(m1, q);
        basicPublishVolatile(m2, q);

        channel.waitForConfirmsOrDie(1000);
        // channel.basicGet(q, false)获取一个消息并且不自动回复ack，如果不手动回复消息，消息不会在服务端被删除。
        long tag1 = checkDelivery(channel.basicGet(q, false), m1, false);
        long tag2 = checkDelivery(channel.basicGet(q, false), m2, false);

        QueueingConsumer consumer = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(q, false, consumer);

        // requeue
        /**
         * basicNack 方法用于拒绝多个消息。它的参数包括：
         * deliveryTag：要拒绝的消息的交付标签。这是一个唯一的标识符，用于标识队列中的特定消息。
         * multiple：一个布尔值，如果设置为 true，则表示拒绝从第一个未确认的消息到指定 deliveryTag 的所有消息；如果设置为 false，则只拒绝指定 deliveryTag 的消息。
         * requeue：一个布尔值，如果设置为 true，则表示被拒绝的消息将被重新排队，等待发送给其他消费者；如果设置为 false，则表示被拒绝的消息将被丢弃或根据队列的设置进行处理。
         * 因此，channel.basicNack(tag2, false, true); 这句话的意思是：拒绝交付标签为 tag2 的消息，并且不拒绝该消息之前的任何消息（因为 multiple 参数为 false），
         * 同时将被拒绝的消息重新排队（requeue=true）或者进入死信队列（requeue=false）
         */
        channel.basicNack(tag2, false, true);

        long tag3 = checkDelivery(consumer.nextDelivery(), m2, true);
        secondaryChannel.basicCancel(consumerTag);

        // no requeue
        secondaryChannel.basicNack(tag3, false, false);

        assertNull(channel.basicGet(q, false));
        channel.basicAck(tag1, false);
        channel.basicNack(tag3, false, true);

        expectError(AMQP.PRECONDITION_FAILED);
    }

    @ParameterizedTest
    @MethodSource("queueCreators")
    public void multiNack(TestUtils.CallableFunction<Channel, String> queueCreator) throws Exception {
        String q = queueCreator.apply(channel);

        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();
        byte[] m3 = "3".getBytes();
        byte[] m4 = "4".getBytes();

        channel.confirmSelect();

        basicPublishVolatile(m1, q);
        basicPublishVolatile(m2, q);
        basicPublishVolatile(m3, q);
        basicPublishVolatile(m4, q);

        channel.waitForConfirmsOrDie(1000);

        checkDelivery(channel.basicGet(q, false), m1, false);
        long tag1 = checkDelivery(channel.basicGet(q, false), m2, false);
        checkDelivery(channel.basicGet(q, false), m3, false);
        long tag2 = checkDelivery(channel.basicGet(q, false), m4, false);

        // ack, leaving a gap in un-acked sequence
        channel.basicAck(tag1, false); // m2消费完成，回复ack，服务端删除

        QueueingConsumer consumer = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(q, false, consumer);

        // requeue multi
        /**
         * 当 multiple 参数设置为 true 时，它表示将拒绝从第一个未确认的消息到指定 deliveryTag（在这个例子中是 tag2）的所有消息。这意味着不仅仅是 tag2 对应的消息会被拒绝，而且在 tag2 之前所有未被确认的消息也会被拒绝。
         * 如果 multiple 参数设置为 false，则只会拒绝指定 deliveryTag 的消息，而不会影响其他消息
         *
         * m1\\m3\m4消息都会被拒绝并且重新如队列，并且发送给consumer
         */
        channel.basicNack(tag2, true, true);

        long tag3 = checkDeliveries(consumer, m1, m3, m4);

        secondaryChannel.basicCancel(consumerTag);

        // no requeue
        secondaryChannel.basicNack(tag3, true, false);

        assertNull(channel.basicGet(q, false));

        channel.basicNack(tag3, true, true);

        expectError(AMQP.PRECONDITION_FAILED);
    }

    @ParameterizedTest
    @MethodSource("queueCreators")
    public void nackAll(TestUtils.CallableFunction<Channel, String> queueCreator) throws Exception {
        String q = queueCreator.apply(channel);

        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();

        channel.confirmSelect();

        basicPublishVolatile(m1, q);
        basicPublishVolatile(m2, q);

        channel.waitForConfirmsOrDie(1000);

        checkDelivery(channel.basicGet(q, false), m1, false);
        checkDelivery(channel.basicGet(q, false), m2, false);

        // nack all
        channel.basicNack(0, true, true);

        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(q, true, c);

        checkDeliveries(c, m1, m2);

        secondaryChannel.basicCancel(consumerTag);
    }

    private long checkDeliveries(QueueingConsumer c, byte[]... messages)
            throws InterruptedException {

        Set<String> msgSet = new HashSet<String>();
        for (byte[] message : messages) {
            msgSet.add(new String(message));
        }

        long lastTag = -1;
        for (int x = 0; x < messages.length; x++) {
            QueueingConsumer.Delivery delivery = c.nextDelivery();
            String m = new String(delivery.getBody());
            assertTrue(msgSet.remove(m), "Unexpected message");
            checkDelivery(delivery, m.getBytes(), true);
            lastTag = delivery.getEnvelope().getDeliveryTag();
        }

        assertTrue(msgSet.isEmpty());
        return lastTag;
    }
}
