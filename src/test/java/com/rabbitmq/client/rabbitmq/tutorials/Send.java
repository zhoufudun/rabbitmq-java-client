package com.rabbitmq.client.rabbitmq.tutorials;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * read
 */
public class Send {

    private final static String QUEUE_NAME = Worker.TASK_QUEUE_NAME;

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setVirtualHost("/zfdtest");
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection()) {
            try (Channel channel = connection.createChannel()) {
//                String queue = channel.queueDeclare(QUEUE_NAME, true, false, false, null).getQueue();
                String message = "Hello World!";
                // 什么情况下会执行回调？有错误的时候，必须服务端发现没有路由等异常会回调handleReturn，否者正常情况下不会执行回调
                channel.addReturnListener(new ReturnListener() {
                    // mandatory当设置为 true 时，如果消息无法路由到任何队列，将会把消息返回给生产者
                    @Override
                    public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        System.out.println("replyCode=" + replyCode);
                        System.out.println("replyText=" + replyText);
                        System.out.println("exchange=" + exchange);
                        System.out.println("routingKey=" + routingKey);
                        System.out.println("properties=" + properties);
                    }
                });
                // mandatory当设置为 true 时，如果消息无法路由到任何队列，将会把消息返回给生产者
                channel.basicPublish("", "noexistroutkey", true, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + message + "'");

//                // 会回调
//                CountDownLatch latch = new CountDownLatch(1);
//                channel.addReturnListener(new ReturnListener() {
//                    @Override
//                    public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                        System.out.println("replyCode="+replyCode);
//                        System.out.println("replyText="+replyText);
//                        System.out.println("exchange="+exchange);
//                        System.out.println("routingKey="+routingKey);
//                        System.out.println("properties="+properties);
//                        latch.countDown();
//                    }
//                });
//                channel.basicPublish("", "notlikelytoexist", true, null, "dummy".getBytes());
//                assertTrue(latch.await(1, TimeUnit.SECONDS), "Should have received returned message");

                Thread.sleep(1000000);
            }

            // 一个AMQConnection会管理N个channel
            try (Channel channel = connection.createChannel()) {
                channel.queueDeclare(QUEUE_NAME+"====2", true, false, false, null);
                String message = "Hello World 2222";
                channel.addReturnListener(new ReturnListener() {
                    @Override
                    public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        System.out.println("replyCode2="+replyCode);
                        System.out.println("replyText2="+replyText);
                        System.out.println("exchange2="+exchange);
                        System.out.println("routingKey2="+routingKey);
                        System.out.println("properties2="+properties);
                    }
                });
                channel.basicPublish("", QUEUE_NAME+"====2", null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent2 '" + message + "'");
            }

        }

        Thread.currentThread().join();
    }
}
