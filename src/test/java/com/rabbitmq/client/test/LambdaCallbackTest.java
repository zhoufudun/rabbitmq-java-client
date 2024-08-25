// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

package com.rabbitmq.client.test;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * read
 */
public class LambdaCallbackTest extends BrokerTestCase {

    String queue;

    @Override
    protected void createResources() throws IOException, TimeoutException {
        queue = channel.queueDeclare("test", true, false, false, null).getQueue();
    }

    @Override
    protected void releaseResources() throws IOException {
        channel.queueDelete(queue);
        try {
            unblock();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // read
    @Test
    public void shutdownListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        try (Connection connection = newConnectionFactory().newConnection()) {
            connection.addShutdownListener(cause -> {
                System.out.println("connection shutdownListener execute");
                latch.countDown();
            });
            Channel channel = connection.createChannel();
            channel.addShutdownListener(cause -> {
                System.out.println("channel shutdownListener execute");
                latch.countDown();
            });
        }
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Connection closed, shutdown listeners should have been called");
    }

    // read
    @Test
    public void confirmListener() throws Exception {
        channel.confirmSelect();
        CountDownLatch latch = new CountDownLatch(1);
        channel.addConfirmListener(
                new ConfirmCallback() {
                    @Override
                    public void handle(long deliveryTag, boolean multiple) throws IOException {
                        System.out.println("ack");
                        latch.countDown();
                    }
                },
                new ConfirmCallback() {
                    @Override
                    public void handle(long deliveryTag, boolean multiple) throws IOException {
                        System.out.println("nack");
                    }
                }
        );
        channel.basicPublish("", "whatever", null, "dummy".getBytes());
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Should have received publisher confirm");
    }

    // read
    @Test
    public void returnListener() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("========" + Thread.currentThread().getName());
                latch.countDown();
            }
        });
        channel.basicPublish("", "notlikelytoexist", true, null, "dummy".getBytes());
        assertTrue(latch.await(1, TimeUnit.SECONDS), "Should have received returned message");
    }

    // 没看明白
    @Test
    public void blockedListener() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (Connection connection = newConnectionFactory().newConnection()) {
            connection.addBlockedListener(
                    reason -> {
                        try {
                            System.out.println("blockedListener callback");
                            unblock();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    },
                    () -> latch.countDown()
            );
            block();
            Channel ch = connection.createChannel();
            ch.basicPublish("", "", null, "dummy".getBytes());
            assertTrue(latch.await(10, TimeUnit.SECONDS), "Should have been blocked and unblocked");
        }
    }

    @Test
    public void basicConsumeDeliverCancel() throws Exception {
        try (Connection connection = newConnectionFactory().newConnection()) {
            final CountDownLatch consumingLatch = new CountDownLatch(1);
            final CountDownLatch cancelLatch = new CountDownLatch(1);
            Channel consumingChannel = connection.createChannel();

            String tag = consumingChannel.basicConsume(queue, true,
                    new DeliverCallback() {
                        @Override
                        public void handle(String consumerTag, Delivery delivery) throws IOException {
                            System.out.println("收到消息， consumerTag=" + consumerTag + ", delivery msg=" + new String(delivery.getBody()));
                            consumingLatch.countDown();
                        }
                    },
                    new CancelCallback() {
                        @Override
                        public void handle(String consumerTag) throws IOException {
                            System.out.println("消息取消回调， consumerTag=" + consumerTag); // 向远程发起删除队列后，远程回返回消息，本客户端收到删除成功消息后会毁掉这里
                            cancelLatch.countDown();
                        }
                    });
            System.out.println("客户端订阅的唯一标识：consumerTag=" + tag);
            this.channel.basicPublish("", queue, null, "dummy".getBytes());
            assertTrue(consumingLatch.await(1, TimeUnit.SECONDS), "deliver callback should have been called");
            this.channel.queueDelete(queue); // 客户端发起请求，服务端回恢复应答
            assertTrue(cancelLatch.await(1, TimeUnit.SECONDS), "cancel callback should have been called");
        }
    }

    @Test
    public void basicConsumeDeliverCancel2() throws Exception {
        try (Connection connection = newConnectionFactory().newConnection()) {
            final CountDownLatch consumingLatch = new CountDownLatch(1);
            final CountDownLatch cancelLatch = new CountDownLatch(1);
            Channel consumingChannel = connection.createChannel();

            String tag = consumingChannel.basicConsume(queue, false,
                    new DeliverCallback() {
                        @Override
                        public void handle(String consumerTag, Delivery delivery) throws IOException {
                            System.out.println("收到消息， consumerTag=" + consumerTag + ", delivery msg=" + new String(delivery.getBody()));
                            // 模拟消息处理失败，不发送确认信号
                            // consumingChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                            consumingLatch.countDown();
                            // requeue表示消息时候会重新入队列（入队列后被重新投递）
                            consumingChannel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
                        }
                    },
                    new CancelCallback() {
                        @Override
                        public void handle(String consumerTag) throws IOException {
                            System.out.println("消息取消回调， consumerTag=" + consumerTag); // 向远程发起删除队列后，远程回返回消息，本客户端收到删除成功消息后会毁掉这里
                            cancelLatch.countDown();
                        }
                    });
            System.out.println("客户端订阅的唯一标识：consumerTag=" + tag);
            Thread.sleep(10000000);
//            this.channel.basicPublish("", queue, null, "dummy".getBytes());
            assertTrue(consumingLatch.await(1, TimeUnit.SECONDS), "deliver callback should have been called");
//            this.channel.queueDelete(queue); // 客户端发起请求，服务端回恢复应答
//            assertTrue(cancelLatch.await(1, TimeUnit.SECONDS), "cancel callback should have been called");
        }
    }

    /**
     * 如何为没有回复ack的消息设置重复投递
     * <p>
     * 解释：
     * 死信队列（DLX）: 当消息在主队列中被拒绝（basicReject）且 requeue=false 时，它会被路由到死信队列。
     * TTL: 死信队列中的消息会在 TTL 过期后被转发回主队列，实现延迟重新投递。
     *
     * @throws Exception
     */
    @Test
    public void NoAutoAck_reconsumer() throws Exception {
        Channel channel = null;
        String mainQueue = "main_queue";
        String dlxQueue = "dlx_queue";
        try (Connection connection = newConnectionFactory().newConnection()) {
            channel = connection.createChannel();

            String dlxExchange = "dlx_exchange";
            String anotherExchange = "another_exchange"; // 新的交换机

            // 1. 配置主队列并绑定到死信交换机
            Map<String, Object> mainQueueArgs = new HashMap<>();
            mainQueueArgs.put("x-dead-letter-exchange", dlxExchange);
            mainQueueArgs.put("x-dead-letter-routing-key", "dlx_routing_key"); // 指定死信路由键
            channel.queueDeclare(mainQueue, true, false, false, mainQueueArgs);

            // 2. 创建死信交换机和死信队列，并绑定到另一个交换机
            Map<String, Object> dlxQueueArgs = new HashMap<>();
            dlxQueueArgs.put("x-message-ttl", 30000); // 设置30秒TTL
            dlxQueueArgs.put("x-dead-letter-exchange", anotherExchange); // 使用另一个交换机
            dlxQueueArgs.put("x-dead-letter-routing-key", "main_queue"); // 重新路由到主队列

            channel.exchangeDeclare(dlxExchange, "direct");
            channel.queueDeclare(dlxQueue, true, false, false, dlxQueueArgs);
            channel.queueBind(dlxQueue, dlxExchange, "dlx_routing_key");

            // 3. 配置新的交换机以将消息路由回主队列
            channel.exchangeDeclare(anotherExchange, "direct");
            channel.queueBind(mainQueue, anotherExchange, "main_queue"); // 绑定新的交换机和主队列

            System.out.println("Main queue and DLX setup completed.");

            // 3. 消费者从主队列消费消息
            Channel finalChannel = channel;
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                try {
                    System.out.println("Received: " + message);

                    // 模拟拒绝消息，并发送到死信队列
                    if (message.contains("reject_condition")) {
                        finalChannel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                        System.out.println("Message rejected and sent to DLX: " + message);
                    } else {
                        // 正常确认消息
                        finalChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        System.out.println("Message acknowledged: " + message);
                    }
                } catch (Exception e) {
                    finalChannel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                    System.out.println("Exception occurred, message rejected.");
                }
            };

            channel.basicConsume(mainQueue, false, deliverCallback, consumerTag -> {
                System.out.println(consumerTag);
            });
            System.out.println("Consumer started, waiting for messages...");

            // 立即消费死信队列中的消息
//            DeliverCallback deliverCallback2 = (consumerTag, delivery) -> {
//                String message = new String(delivery.getBody(), "UTF-8");
//                System.out.println("Received from DLQ: " + message);
//
//                // 重新发送到主队列
//                channel.basicPublish("", mainQueue, null, message.getBytes());
//                System.out.println("Message resent to main queue.");
//
//                // 手动确认消息
//                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//            };
//
//            channel.basicConsume(dlxQueue, false, deliverCallback2, consumerTag -> {
//                System.out.println("=========");
//            });
//            System.out.println("Resend consumer started.");

            Thread.currentThread().join(); // 保持主线程活跃
        } catch (Exception e) {
            channel.queueDelete(mainQueue);
            channel.queueDelete(dlxQueue);
        }
    }

    @Test
    public void publishMessage() throws Exception {
        try (Connection connection = newConnectionFactory().newConnection()) {
            Channel channel = connection.createChannel();
            channel.basicPublish("", "main_queue", null, "reject_condition".getBytes());
            System.out.println("Message published to main_queue.");
        }
    }

    @Test
    public void basicConsumeDeliverManualAck() throws Exception {
        try (Connection connection = newConnectionFactory().newConnection()) {
            final CountDownLatch consumingLatch = new CountDownLatch(1);
            final CountDownLatch cancelLatch = new CountDownLatch(1);
            Channel consumingChannel = connection.createChannel();

            String tag = consumingChannel.basicConsume(queue, false,
                    new DeliverCallback() {
                        @Override
                        public void handle(String consumerTag, Delivery delivery) throws IOException {
                            System.out.println("收到消息， consumerTag=" + consumerTag + ", delivery msg=" + new String(delivery.getBody()));
                            consumingLatch.countDown();

                            consumingChannel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                        }
                    },
                    new CancelCallback() {
                        @Override
                        public void handle(String consumerTag) throws IOException {
                            System.out.println("消息取消回调， consumerTag=" + consumerTag); // 向远程发起删除队列后，远程回返回消息，本客户端收到删除成功消息后会毁掉这里
                            cancelLatch.countDown();
                        }
                    });
            System.out.println("客户端订阅的唯一标识：consumerTag=" + tag);
            Thread.sleep(100000);
        }
    }

    @Test
    public void basicConsumeDeliverShutdown() throws Exception {
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        try (Connection connection = newConnectionFactory().newConnection()) {
            final CountDownLatch consumingLatch = new CountDownLatch(1);
            Channel consumingChannel = connection.createChannel();
            consumingChannel.basicConsume(queue, true,
                    (consumerTag, delivery) -> consumingLatch.countDown(),
                    (consumerTag, sig) -> shutdownLatch.countDown()
            );
            this.channel.basicPublish("", queue, null, "dummy".getBytes());
            assertTrue(consumingLatch.await(1, TimeUnit.SECONDS), "deliver callback should have been called");
        }
        assertTrue(shutdownLatch.await(1, TimeUnit.SECONDS), "shutdown callback should have been called");
    }

    @Test
    public void basicConsumeCancelDeliverShutdown() throws Exception {
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        try (Connection connection = TestUtils.connectionFactory().newConnection()) {
            final CountDownLatch consumingLatch = new CountDownLatch(1);
            Channel consumingChannel = connection.createChannel();
            // not both cancel and shutdown callback can be called on the same consumer
            // testing just shutdown
            consumingChannel.basicConsume(queue, true,
                    (consumerTag, delivery) -> consumingLatch.countDown(),
                    (consumerTag) -> {
                    },
                    (consumerTag, sig) -> shutdownLatch.countDown()
            );
            this.channel.basicPublish("", queue, null, "dummy".getBytes());
            assertTrue(consumingLatch.await(1, TimeUnit.SECONDS), "deliver callback should have been called");
        }
        assertTrue(shutdownLatch.await(1, TimeUnit.SECONDS), "shutdown callback should have been called");
    }

}
