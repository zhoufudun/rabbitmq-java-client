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
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.test.BrokerTestCase;

/**
 * read
 */
public class QueueLease extends BrokerTestCase {

    private final static String TEST_EXPIRE_QUEUE = "leaseq";
    private final static String TEST_NORMAL_QUEUE = "noleaseq";
    private final static String TEST_EXPIRE_REDECLARE_QUEUE = "equivexpire";

    // Currently the expiration timer is very responsive but this may
    // very well change in the future, so tweak accordingly.
    private final static int QUEUE_EXPIRES = 1000; // msecs
    private final static int SHOULD_EXPIRE_WITHIN = 2000;

    /**
     * Verify that a queue with the 'x-expires` flag is actually deleted within
     * a sensible period of time after expiry.
     */
    @Test
    public void queueExpires() throws IOException, InterruptedException {
        verifyQueueExpires(TEST_EXPIRE_QUEUE, true);
    }

    /**
     * Verify that the server does not delete normal queues... ;)
     */
    @Test
    public void doesNotExpireOthers() throws IOException, InterruptedException {
        verifyQueueExpires(TEST_NORMAL_QUEUE, false);
    }

    @Test
    public void expireMayBeByte() throws IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", (byte) 100); // 将一个值为100的byte类型数据放入args中，这个值代表队列的过期时间（毫秒）

        try {
            channel.queueDeclare("expiresMayBeByte", false, true, false, args);
        } catch (IOException e) {
            fail("server did not accept x-expires of type byte");
        }
    }

    @Test
    public void expireMayBeShort() throws IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", (short) 100); // 将一个值为100的short类型数据放入args中，这个值代表队列的过期时间（毫秒）

        try {
            channel.queueDeclare("expiresMayBeShort", false, true, false, args);
        } catch (IOException e) {
            fail("server did not accept x-expires of type short");
        }
    }

    @Test
    public void expireMayBeLong() throws IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", 100L); // 将一个值为100的long类型数据放入args中，这个值代表队列的过期时间（毫秒）

        try {
            channel.queueDeclare("expiresMayBeLong", false, true, false, args);
        } catch (IOException e) {
            fail("server did not accept x-expires of type long");
        }
    }

    @Test
    public void expireMustBeGtZero() throws IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", 0); // 值必须大于0，否者会抛出异常

        try {
            channel.queueDeclare("expiresMustBeGtZero", false, false, false, args);
            fail("server accepted x-expires of zero ms.");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }
    }

    @Test
    public void expireMustBePositive() throws IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", -10); // 服务端不接受负值，否者会抛出异常

        try {
            channel.queueDeclare("expiresMustBePositive", false, false, false, args);
            fail("server accepted negative x-expires.");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }
    }

    /**
     * Verify that the server throws an error if the client redeclares a queue
     * with mismatching 'x-expires' values.
     */
    @Test
    public void queueRedeclareEquivalence() throws IOException {
        Map<String, Object> args1 = new HashMap<String, Object>();
        args1.put("x-expires", 10000);
        Map<String, Object> args2 = new HashMap<String, Object>();
        args2.put("x-expires", 20000);

        channel.queueDeclare(TEST_EXPIRE_REDECLARE_QUEUE, false, false, false, args1);

        try {
            channel.queueDeclare(TEST_EXPIRE_REDECLARE_QUEUE, false, false, false, args2);
            fail("Able to redeclare queue with mismatching expire flags.");
        } catch (IOException e) {
            // 两次声明队列的的参数不匹配，所以会抛出异常
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e); // PRECONDITION 翻译为中文是 "前提条件"，是指在满足一定条件之前，无法进行的操作。
        }
    }

    @Test
    public void activeQueueDeclareExtendsLease()
            throws InterruptedException, IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", QUEUE_EXPIRES);
        channel.queueDeclare(TEST_EXPIRE_QUEUE, false, false, false, args);

        Thread.sleep(QUEUE_EXPIRES * 3 / 4);
        try {
            // 队列的过期时间是1000毫秒，但是这里休眠了3/4，所以队列的过期时间还没有到
            channel.queueDeclare(TEST_EXPIRE_QUEUE, false, false, false, args);
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            fail("Queue expired before active re-declaration.");
        }

        // 又休眠了3/4，所以队列的过期时间到了
        Thread.sleep(QUEUE_EXPIRES * 3 / 4);
        try {
            channel.queueDeclarePassive(TEST_EXPIRE_QUEUE); // 队列不存在并且设置了排他属性，才会抛异常
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            fail("Queue expired: active re-declaration did not extend lease.");
        }
    }

    @Test
    public void passiveQueueDeclareExtendsLease()
            throws InterruptedException, IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", QUEUE_EXPIRES);
        channel.queueDeclare(TEST_EXPIRE_QUEUE, false, false, false, args);
        // 队列的过期时间是1000毫秒，但是这里休眠了3/4，所以队列的过期时间还没有到
        Thread.sleep(QUEUE_EXPIRES * 3 / 4);
        try {
            channel.queueDeclarePassive(TEST_EXPIRE_QUEUE); // 队列未过期，这里不会抛异常
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            fail("Queue expired before before passive re-declaration.");
        }

        Thread.sleep(QUEUE_EXPIRES * 3 / 4);
        // 队列已经过期
        try {
            channel.queueDeclarePassive(TEST_EXPIRE_QUEUE); // 队列已经过期吗，但是不排他，不会抛异常
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            fail("Queue expired: passive redeclaration did not extend lease.");
        }
    }

    @Test
    public void expiresWithConsumers()
            throws InterruptedException, IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-expires", QUEUE_EXPIRES);
        channel.queueDeclare(TEST_EXPIRE_QUEUE, false, false, false, args);

        Consumer consumer = new DefaultConsumer(channel);
        String consumerTag = channel.basicConsume(TEST_EXPIRE_QUEUE, consumer);

        Thread.sleep(SHOULD_EXPIRE_WITHIN); // 等待队列过期
        try {
            channel.queueDeclarePassive(TEST_EXPIRE_QUEUE); // 队列不存在，但是不排他属性，所以不会抛异常
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            fail("Queue expired before before passive re-declaration.");
        }

        channel.basicCancel(consumerTag); // 取消消费者
        Thread.sleep(SHOULD_EXPIRE_WITHIN);
        try {
            channel.queueDeclarePassive(TEST_EXPIRE_QUEUE); // 队列不存在，但是不排他属性，所以不会抛异常
            fail("Queue should have been expired by now.");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
        }
    }

    void verifyQueueExpires(String name, boolean expire) throws IOException,
            InterruptedException {
        Map<String, Object> args = new HashMap<String, Object>();
        if (expire) {
            args.put("x-expires", QUEUE_EXPIRES); // 设置队列过期时间1s
        }

        channel.queueDeclare(name, false, false, false, args);

        Thread.sleep(SHOULD_EXPIRE_WITHIN / 4); // 等待0.5s

        try {
            channel.queueDeclarePassive(name); // 不抛异常才是对的，因为队列还没过期了
            // 如果队列没有过期，那么这里应该抛出异常
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            fail("Queue expired before deadline.");
        }

        Thread.sleep(SHOULD_EXPIRE_WITHIN); // be on the safe side， 此时队列应该过期了

        try {
            channel.queueDeclarePassive(name); // 如果队列过期了（队列不存在），那么这里应该抛出异常
            if (expire) {
                fail("Queue should have been expired by now.");
            }
        } catch (IOException e) {
            if (expire) {
                checkShutdownSignal(AMQP.NOT_FOUND, e);
            } else {
                fail("Queue without expire flag deleted.");
            }
        }
    }

    protected void releaseResources() throws IOException {
        try {
            channel.queueDelete(TEST_NORMAL_QUEUE);
            channel.queueDelete(TEST_EXPIRE_QUEUE);
            channel.queueDelete(TEST_EXPIRE_REDECLARE_QUEUE);
        } catch (IOException e) {
        }

        super.releaseResources();
    }
}
