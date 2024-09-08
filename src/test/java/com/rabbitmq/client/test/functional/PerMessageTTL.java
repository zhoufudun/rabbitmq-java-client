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

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * read
 */
public class PerMessageTTL extends TTLHandling {

    protected Object sessionTTL;

    @Override
    protected void publish(String msg) throws IOException {
        /**
         * expiration：这是一个消息属性，用于设置单个消息的过期时间。当消息被发布时，
         * 可以通过 MessageProperties 设置 expiration 属性，这样只有这一条消息会在指定的时间后过期
         */
        basicPublishVolatile(msg.getBytes(), TTL_EXCHANGE, TTL_QUEUE_NAME,
                // 发送消息是为某条消息单独设置过期属性
                MessageProperties.TEXT_PLAIN
                        .builder()
                        .expiration(String.valueOf(sessionTTL))
                        .build());
    }

    @Override
    protected AMQP.Queue.DeclareOk declareQueue(String name, Object ttlValue) throws IOException {
        this.sessionTTL = ttlValue;
        return this.channel.queueDeclare(name, false, true, false, null);
    }

    @Test
    public void expiryWhenConsumerIsLateToTheParty() throws Exception {
        deleteQueue(TTL_QUEUE_NAME);

        declareAndBindQueue(500); // sessionTTL==500ms

        publish(MSG[0]); // 发布消息，过期时间500ms
        this.sessionTTL = 100;
        publish(MSG[1]); // 发布消息，过期时间100ms

        Thread.sleep(200); // 等待100ms，消息过期

        QueueingConsumer c = new QueueingConsumer(channel);
        channel.basicConsume(TTL_QUEUE_NAME, true, c);

        assertNotNull(c.nextDelivery(100), "Message unexpectedly expired"); // MSG[0]还没过期

        // MSG[1]已过期，并且MSG[0]已经acknowledged没法被获取
        QueueingConsumer.Delivery delivery = c.nextDelivery(100);
        assertNull(delivery, "Message should have been expired!!");

    }

    @Test
    public void restartingExpiry() throws Exception {
        final String expiryDelay = "2000";
        declareDurableQueue(TTL_QUEUE_NAME);
        bindQueue();
        channel.basicPublish(TTL_EXCHANGE, TTL_QUEUE_NAME,
                MessageProperties.MINIMAL_PERSISTENT_BASIC
                        .builder()
                        .expiration(expiryDelay) // 设置消息过期时间2s
                        .build(), new byte[]{});
        restart(); // 重启rabbitmq服务器，过期的消息会被删除
        Thread.sleep(Integer.parseInt(expiryDelay));
        try {
            // 重新消费，理论消息已经过期了，获取不到
            assertNull(get(), "Message should have expired after broker restart");
        } finally {
            deleteQueue(TTL_QUEUE_NAME);
        }
    }

}
