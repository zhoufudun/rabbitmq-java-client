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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
/**
 * read
 */
public class PerQueueVsPerMessageTTL extends PerMessageTTL {

    @Test
    public void smallerPerQueueExpiryWins() throws IOException, InterruptedException {
        declareAndBindQueue(10); // 设置队列的所有消息ttl=10ms
        this.sessionTTL = 1000;

        publish("message1"); // 设置消息的ttl=1000

        Thread.sleep(100); // 等待10ms，等队列过期

        // 到这里经过了100ms，队列过期了，队列的所有消息过期了，消息【message1】也过期了
        assertNull(get(), "per-queue ttl should have removed message after 10ms");
    }

    @Override
    protected AMQP.Queue.DeclareOk declareQueue(String name, Object ttlValue) throws IOException {
        final Object mappedTTL = (ttlValue instanceof String &&
                ((String) ttlValue).contains("foobar")) ?
                ttlValue : longValue(ttlValue) * 2;
        this.sessionTTL = ttlValue;
        /**
         * x-message-ttl -> 10
         * x-message-ttl：这是一个队列参数，用于设置队列中所有消息的过期时间。
         * 当消息被发布到队列时，如果队列设置了 x-message-ttl，那么所有消息都会在这个时间后过期
         */
        Map<String, Object> argMap = Collections.singletonMap(PerQueueTTL.TTL_ARG, mappedTTL);
        return this.channel.queueDeclare(name, false, true, false, argMap);
    }

    private Long longValue(final Object ttl) {
        if (ttl instanceof Short) {
            return ((Short) ttl).longValue();
        } else if (ttl instanceof Integer) {
            return ((Integer) ttl).longValue();
        } else if (ttl instanceof Long) {
            return (Long) ttl;
        } else {
            throw new IllegalArgumentException("ttl not of expected type");
        }
    }

}
