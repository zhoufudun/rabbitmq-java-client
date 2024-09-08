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
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.MessageProperties;

/**
 * read
 */
public class PerQueueTTL extends TTLHandling {

    protected static final String TTL_ARG = "x-message-ttl";

    @Override
    protected AMQP.Queue.DeclareOk declareQueue(String name, Object ttlValue) throws IOException {
        /**
         * x-message-ttl -> 10
         * x-message-ttl：这是一个队列参数，用于设置队列中所有消息的过期时间。
         * 当消息被发布到队列时，如果队列设置了 x-message-ttl，那么所有消息都会在这个时间后过期
         */
        Map<String, Object> argMap = Collections.singletonMap(TTL_ARG, ttlValue);
        return this.channel.queueDeclare(name, false, true, false, argMap);
    }

    @Test
    public void queueReDeclareEquivalence() throws Exception {
        declareQueue(10); // 设置队列的消息ttl
        try {
            declareQueue(20); // 无法redeclare with different x-message-ttl
            fail("Should not be able to redeclare with different x-message-ttl");
        } catch (IOException ex) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, ex);
        }
    }

    /**
     * 测试不同类型的值进行声明ttl，理论上不会报错
     * @throws Exception
     */
    @Test
    public void queueReDeclareSemanticEquivalence() throws Exception {
        declareQueue((byte) 10);
        declareQueue(10);
        declareQueue((short) 10);
        declareQueue(10L);
    }

    @Test
    public void queueReDeclareSemanticNonEquivalence() throws Exception {
        declareQueue(10);
        try {
            declareQueue(10.0); // 报错
            fail("Should not be able to redeclare with x-message-ttl argument of different type");
        } catch (IOException ex) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, ex);
        }
    }

    protected void publishWithExpiration(String msg, Object sessionTTL) throws IOException {
        basicPublishVolatile(msg.getBytes(), TTL_EXCHANGE, TTL_QUEUE_NAME,
                MessageProperties.TEXT_PLAIN
                        .builder()
                        .expiration(String.valueOf(sessionTTL))
                        .build());
    }
}
