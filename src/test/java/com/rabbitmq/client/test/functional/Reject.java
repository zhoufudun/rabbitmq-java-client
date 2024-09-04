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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.test.TestUtils;
import com.rabbitmq.client.test.TestUtils.CallableFunction;

import java.util.Collections;

import java.util.UUID;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * read
 */
public class Reject extends AbstractRejectTest {

    public static Object[] reject() {
        return new Object[]{
                (CallableFunction<Channel, String>) channel -> {
                    String quorum = channel.queueDeclare(UUID.randomUUID().toString(), true, false, false, Collections.singletonMap("x-queue-type", "quorum")).getQueue();
                    return quorum;
                },
                (CallableFunction<Channel, String>) channel -> {
                    String classic = channel.queueDeclare(UUID.randomUUID().toString(), true, false, false, Collections.singletonMap("x-queue-type", "classic")).getQueue();
                    return classic;
                }};
    }

    @ParameterizedTest
    @MethodSource(value = "reject")
    public void reject(TestUtils.CallableFunction<Channel, String> queueCreator)
            throws Exception {
        String queue = queueCreator.apply(channel);

        channel.confirmSelect(); // 设置需要等待确认

        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();

        basicPublishVolatile(m1, queue);
        basicPublishVolatile(m2, queue);

        channel.waitForConfirmsOrDie(1000); // 等待确认，超时会断开connection

        long tag1 = checkDelivery(channel.basicGet(queue, false), m1, false);
        long tag2 = checkDelivery(channel.basicGet(queue, false), m2, false);

        QueueingConsumer consumer = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(queue, false, consumer);
        channel.basicReject(tag2, true); // 拒绝第二个消息，并且重新入队列
        long tag3 = checkDelivery(consumer.nextDelivery(), m2, true); // 此时再次消费者获取到的消息是m2，并且是重新入队列的消息

        secondaryChannel.basicCancel(consumerTag); // 取消消费者
        secondaryChannel.basicReject(tag3, false); // 拒绝第三个消息，并且不重新入队列
        assertNull(channel.basicGet(queue, false)); // 此时队列中没有消息，应为第三个消息被拒绝，并且不重新入队列
        channel.basicAck(tag1, false); // 确认第一个消息
        channel.basicReject(tag3, false);// 拒绝第三个消息，并且不重新入队列
        expectError(AMQP.PRECONDITION_FAILED);
    }
}
