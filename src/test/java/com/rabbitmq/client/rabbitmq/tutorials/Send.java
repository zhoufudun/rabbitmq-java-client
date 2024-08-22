package com.rabbitmq.client.rabbitmq.tutorials;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                String message = "Hello World!";
                channel.addReturnListener(new ReturnListener() {
                    @Override
                    public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        System.out.println("replyCode="+replyCode);
                        System.out.println("replyText="+replyText);
                        System.out.println("exchange="+exchange);
                        System.out.println("routingKey="+routingKey);
                        System.out.println("properties="+properties);
                    }
                });
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + message + "'");
            }
        }
    }
}
