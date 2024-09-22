package com.rabbitmq.client.rabbitmq.tutorials;

import com.rabbitmq.client.*;
import com.rabbitmq.client.test.TestUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

import java.util.stream.Stream;

/**
 * read
 */
public class ReceiveLogsDirectTest {

    private static final String EXCHANGE_NAME = "direct_logs";

    static class MyArgumentsProvider implements ArgumentsProvider {

        @Override
        public java.util.stream.Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            // 这里返回一个包含测试参数的Stream
            return Stream.of(Arguments.of("param1"), Arguments.of("param2"));
        }
    }
    public static String[] getArgv(){
        return new String[]{"111,222"};
    }

    public static void main(String[] args) {

    }
    @ParameterizedTest
//    @ValueSource(strings = {"111,222"})
    @ArgumentsSource(MyArgumentsProvider.class)
    public void test2(String argv) throws Exception {
        System.out.println(argv);
    }

    @ParameterizedTest
    @ValueSource(strings = {"111"})
    public void test1(String argv) throws Exception {
        ConnectionFactory factory = TestUtils.connectionFactory();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare("ReceiveLogsDirectTest",false,false,true,null).getQueue();

        if(argv==null || argv.length()==0){
            System.exit(1);
        }

        channel.queueBind(queueName, EXCHANGE_NAME, argv);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

        Thread.sleep(500000);
    }
}

