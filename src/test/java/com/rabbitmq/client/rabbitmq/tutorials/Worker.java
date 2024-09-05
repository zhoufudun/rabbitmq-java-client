package com.rabbitmq.client.rabbitmq.tutorials;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.test.TestUtils;

public class Worker {

    public static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = TestUtils.connectionFactory();
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        /**
         * TASK_QUEUE_NAME：队列的名称。
         * 	•	true：durable 参数，如果设置为 true，队列在消息代理（broker）重启后仍然存在。
         * 	•	false：exclusive 参数，如果设置为 true，该队列仅对声明它的连接可见，并且在连接关闭时自动删除。
         * 	•	false：autoDelete 参数，如果设置为 true，当队列不再使用时（例如最后一个消费者取消订阅时），队列会被自动删除。
         * 	•	null：可选参数，用于设置额外的属性，如消息的存活时间（TTL）、最大长度等。
         */
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        // 即每次只处理一个消息，这确保了在处理消息时，不会有其他消息被并发处理，直到当前消息处理完成
        channel.basicQos(1);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            System.out.println(" [x] Received '" + message + "'");
            try {
                doWork(message);
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

