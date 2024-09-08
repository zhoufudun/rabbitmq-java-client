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

import static com.rabbitmq.client.test.functional.QosTests.drain;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.test.BrokerTestCase;

/**
 * read
 */
public class PerConsumerPrefetch extends BrokerTestCase {
    private String q;

    @Override
    protected void createResources() throws IOException {
        q = channel.queueDeclare().getQueue();
    }

    private interface Closure {
        public void makeMore(List<Delivery> deliveries) throws IOException;
    }

    @Test
    public void singleAck() throws IOException {
        testPrefetch(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                for (Delivery del : deliveries) {
                    ack(del, false);
                }
            }
        });
    }

    @Test
    public void multiAck() throws IOException {
        testPrefetch(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                ack(deliveries.get(deliveries.size() - 1), true); // 最后一条消息回复ack，那么表示当前消息和之前的消息都已经被确认
            }
        });
    }

    @Test
    public void singleNack_requeue_false() throws IOException {
        for (final boolean requeue : Arrays.asList(false)) { // false表示不重新入队，true表示重新入队
            testPrefetch_nack(new Closure() {
                public void makeMore(List<Delivery> deliveries) throws IOException {
                    for (Delivery del : deliveries) {
                        nack(del, false, requeue);
                    }
                }
            }, requeue);
        }
    }

    @Test
    public void singleNack_requeue_true() throws IOException {
        for (final boolean requeue : Arrays.asList(true)) { // false表示不重新入队，true表示重新入队
            testPrefetch_nack(new Closure() {
                public void makeMore(List<Delivery> deliveries) throws IOException {
                    for (Delivery del : deliveries) {
                        nack(del, false, requeue);
                    }
                }
            }, requeue);
        }
    }

    @Test
    public void multiNack_requeue_false() throws IOException {
        for (final boolean requeue : Arrays.asList(false)) {
            testPrefetch_nack(new Closure() {
                public void makeMore(List<Delivery> deliveries) throws IOException {
                    nack(deliveries.get(deliveries.size() - 1), true, requeue);
                }
            },requeue);
        }
    }

    @Test
    public void multiNack_requeue_true() throws IOException {
        for (final boolean requeue : Arrays.asList(true)) {
            testPrefetch_nack(new Closure() {
                public void makeMore(List<Delivery> deliveries) throws IOException {
                    nack(deliveries.get(deliveries.size() - 1), true, requeue);
                }
            },requeue);
        }
    }

    @Test
    public void recover() throws IOException {
        testPrefetch_recover(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                channel.basicRecover(); // 重新这一批消息
            }
        });
    }

    /**
     * 测试prefetch参数
     *
     * @param closure
     * @throws IOException
     */
    private void testPrefetch(Closure closure) throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 15); // 发布15条消息
        consume(c, 5, false); // 一次性获取5条消息
        List<Delivery> deliveries = drain(c, 5); // 阻塞，一次性获取5条消息【0~4】

        ack(channel.basicGet(q, false), false); // 主动获取一条数据，理论上这个消息是5，并且回复ack
        drain(c, 0); // 5条数据没回复ack，因此在再次获取的时候是获取不到的

        closure.makeMore(deliveries); // 一次性回复前五个消息的ack，或者回复最后一个消息的ack（前提是multi=true）
        List<Delivery> drain = drain(c, 5);// 前五条消息回复了ack，因此在再次获取的时候可以获取到，理论上获取到消息为【6,7,8,9,10】
        List<String> collect = drain.stream().map(new Function<Delivery, String>() {
            @Override
            public String apply(Delivery delivery) {
                return new String(delivery.getBody());
            }
        }).collect(Collectors.toList());

        assertEquals(Arrays.asList("6", "7", "8", "9", "10"), collect);
    }

    /**
     * 测试prefetch参数
     *
     * @param closure
     * @throws IOException
     */
    private void testPrefetch_recover(Closure closure) throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 15); // 发布15条消息
        consume(c, 5, false); // 一次性获取5条消息
        List<Delivery> deliveries = drain(c, 5); // 阻塞，一次性获取5条消息【0~4】

        ack(channel.basicGet(q, false), false); // 主动获取一条数据，理论上这个消息是5，并且回复ack
        drain(c, 0); // 5条数据没回复ack，因此在再次获取的时候是获取不到的

        closure.makeMore(deliveries); // 使获取到的消息recover
        List<Delivery> drain = drain(c, 5);// recover，理论上再次获取到消息为【0,1,2,3,4】
        List<String> collect = drain.stream().map(new Function<Delivery, String>() {
            @Override
            public String apply(Delivery delivery) {
                return new String(delivery.getBody());
            }
        }).collect(Collectors.toList());

        assertEquals(Arrays.asList("0", "1", "2", "3", "4"), collect);
    }

    /**
     * 测试prefetch参数
     *
     * @param closure
     * @throws IOException
     */
    private void testPrefetch_nack(Closure closure, boolean requeue) throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 15); // 发布15条消息
        consume(c, 5, false); // 一次性获取5条消息
        List<Delivery> deliveries = drain(c, 5); // 阻塞，一次性获取5条消息【0~4】

        ack(channel.basicGet(q, false), false); // 主动获取一条数据，理论上这个消息是5，并且回复ack
        List<Delivery> drain1 = drain(c, 0);// 5条数据没回复ack，因此在再次获取的时候是获取不到的
        assertEquals(0, drain1.size());

        closure.makeMore(deliveries); // 一次性回复前五个消息的nack,表示重新入队或者不入队
        List<Delivery> drain = drain(c, 5);// 前五条消息回复了nack，因此在再次获取的时候可以获取到，理论上如果不重新如果获取到消息为【6,7,8,9,10】或者如果重新入队获取到消息为【6,7,8,9,10】
        List<String> collect = drain.stream().map(new Function<Delivery, String>() {
            @Override
            public String apply(Delivery delivery) {
                return new String(delivery.getBody());
            }
        }).collect(Collectors.toList());

        if (!requeue) {
            assertEquals(Arrays.asList("6", "7", "8", "9", "10"), collect);
        } else {
            assertEquals(Arrays.asList("0", "1", "2", "3", "4"), collect);
        }
    }

    @Test
    public void prefetchOnEmpty() throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 5);
        consume(c, 10, false);
        drain(c, 5);
        publish(q, 10);
        drain(c, 5);
    }

    @Test
    public void autoAckIgnoresPrefetch() throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 10);
        consume(c, 1, true);
        drain(c, 10);
    }

    @Test
    public void prefetchZeroMeansInfinity() throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 10);
        consume(c, 0, false);
        drain(c, 10);
    }

    private void publish(String q, int n) throws IOException {
        for (int i = 0; i < n; i++) {
            channel.basicPublish("", q, null, (i + "").getBytes());
        }
    }

    private void consume(QueueingConsumer c, int prefetch, boolean autoAck) throws IOException {
        channel.basicQos(prefetch);
        channel.basicConsume(q, autoAck, c);
    }

    private void ack(Delivery del, boolean multi) throws IOException {
        channel.basicAck(del.getEnvelope().getDeliveryTag(), multi);
    }

    private void ack(GetResponse get, boolean multi) throws IOException {
        channel.basicAck(get.getEnvelope().getDeliveryTag(), multi);
    }

    private void nack(Delivery del, boolean multi, boolean requeue) throws IOException {
        channel.basicNack(del.getEnvelope().getDeliveryTag(), multi, requeue);
    }
}
