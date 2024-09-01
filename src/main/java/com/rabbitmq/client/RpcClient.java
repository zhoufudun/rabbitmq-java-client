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

package com.rabbitmq.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import com.rabbitmq.client.impl.MethodArgumentReader;
import com.rabbitmq.client.impl.MethodArgumentWriter;
import com.rabbitmq.client.impl.ValueReader;
import com.rabbitmq.client.impl.ValueWriter;
import com.rabbitmq.utility.BlockingCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class which manages simple RPC-style communication.
 * The class is agnostic about the format of RPC arguments / return values.
 * It simply provides a mechanism for sending a message to an exchange with a given routing key,
 * and waiting for a response.
 */
public class RpcClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);

    /**
     * Channel we are communicating on
     */
    private final Channel _channel;
    /**
     * Exchange to send requests to
     */
    private final String _exchange;
    /**
     * Routing key to use for requests
     */
    private final String _routingKey;
    /**
     * Queue where the server should put the reply
     *
     * _replyTo 在 AMQP（高级消息队列协议）中是一个非常重要的概念，特别是在 RPC（远程过程调用）模式中。在这段 Java 代码中，_replyTo 是 RpcClientParams 对象的一个属性，它定义了用于接收服务器响应的队列名称。
     * 当客户端向服务器发送 RPC 请求时，它会在消息的 replyTo 属性中指定一个队列名称，这个队列是客户端用来接收来自服务器的响应的。服务器处理完请求后，会将响应发送到这个指定的队列。客户端则会监听这个队列，以获取服务器返回的响应。
     * 在 RabbitMQ 中，_replyTo 通常是一个临时队列的名称，这个队列会在客户端连接到 RabbitMQ 时创建，并在客户端断开连接时自动删除。这样可以确保每个 RPC 请求都有一个唯一的响应队列，并且不会因为队列名称冲突而导致消息混乱。
     * 在这段代码中，_replyTo 的值是通过 params.getReplyTo() 方法获取的，这个方法可能是从配置文件、环境变量或者其他方式获取的队列名称。然后，这个值被用来设置 RpcClient 的 _replyTo 属性，以便在发送 RPC 请求时使用
     *
     *在 RabbitMQ 中，如果使用 amq.rabbitmq.reply-to 作为 replyTo 队列的值，那么客户端无需显式声明该队列。amq.rabbitmq.reply-to 是 RabbitMQ 内置的一个特殊队列，它允许客户端实现简化的 RPC 通信模式。这个队列是由 RabbitMQ 自动管理的，客户端只需订阅它，无需手动声明
     */
    private final String _replyTo;
    /**
     * timeout to use on call responses
     */
    private final int _timeout;
    /**
     * NO_TIMEOUT value must match convention on {@link BlockingCell#uninterruptibleGet(int)}
     */
    protected final static int NO_TIMEOUT = -1;
    /**
     * Whether to publish RPC requests with the mandatory flag or not.
     */
    private final boolean _useMandatory;
    /**
     * closed flag
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public final static Function<Object, Response> DEFAULT_REPLY_HANDLER = reply -> {
        if (reply instanceof ShutdownSignalException) {
            ShutdownSignalException sig = (ShutdownSignalException) reply;
            ShutdownSignalException wrapper =
                    new ShutdownSignalException(sig.isHardError(),
                            sig.isInitiatedByApplication(),
                            sig.getReason(),
                            sig.getReference());
            wrapper.initCause(sig);
            throw wrapper;
        } else if (reply instanceof UnroutableRpcRequestException) {
            throw (UnroutableRpcRequestException) reply;
        } else {
            return (Response) reply;
        }
    };

    private final Function<Object, Response> _replyHandler;

    /**
     * Map from request correlation ID to continuation BlockingCell
     */
    private final Map<String, BlockingCell<Object>> _continuationMap = new HashMap<String, BlockingCell<Object>>();

    /**
     * Generates correlation ID for each request.
     *
     * @since 5.9.0
     */
    private final Supplier<String> _correlationIdSupplier;
    private final ReturnListener _returnListener;

    private String lastCorrelationId = "0";

    /**
     * Consumer attached to our reply queue
     */
    private final DefaultConsumer _consumer;

    /**
     * Construct a {@link RpcClient} with the passed-in {@link RpcClientParams}.
     *
     * @param params
     * @throws IOException
     * @see RpcClientParams
     * @since 5.6.0
     */
    public RpcClient(RpcClientParams params) throws
            IOException {
        _channel = params.getChannel(); // AMQChannel(amqp://guest@0:0:0:0:0:0:0:1:5672//zfdtest,1)
        _exchange = params.getExchange();//""
        _routingKey = params.getRoutingKey();// rpc.queue
        _replyTo = params.getReplyTo(); // amq.rabbitmq.reply-to
        if (params.getTimeout() < NO_TIMEOUT) {
            throw new IllegalArgumentException("Timeout argument must be NO_TIMEOUT(-1) or non-negative.");
        }
        _timeout = params.getTimeout(); // 1000
        _useMandatory = params.shouldUseMandatory(); // false
        _replyHandler = params.getReplyHandler(); // 服务端回复后消息处理组件
        _correlationIdSupplier = params.getCorrelationIdSupplier();

        _consumer = setupConsumer(); // 注册消费者
        if (_useMandatory) {
            this._returnListener = this._channel.addReturnListener(returnMessage -> {
                synchronized (_continuationMap) {
                    String replyId = returnMessage.getProperties().getCorrelationId();
                    BlockingCell<Object> blocker = _continuationMap.remove(replyId);
                    if (blocker == null) {
                        // Entry should have been removed if request timed out,
                        // log a warning nevertheless.
                        LOGGER.warn("No outstanding request for correlation ID {}", replyId);
                    } else {
                        blocker.set(new UnroutableRpcRequestException(returnMessage));
                    }
                }
            });
        } else {
            this._returnListener = null;
        }
    }

    /**
     * Private API - ensures the RpcClient is correctly open.
     *
     * @throws IOException if an error is encountered
     */
    private void checkNotClosed() throws IOException {
        if (this.closed.get()) {
            throw new EOFException("RpcClient is closed");
        }
    }

    /**
     * Public API - cancels the consumer, thus deleting the temporary queue, and marks the RpcClient as closed.
     *
     * @throws IOException if an error is encountered
     */
    @Override
    public void close() throws IOException {
        if (this.closed.compareAndSet(false, true)) {
            _channel.basicCancel(_consumer.getConsumerTag());
            if (this._returnListener != null) {
                _channel.removeReturnListener(this._returnListener);
            }
        }
    }

    /**
     * Registers a consumer on the reply queue. 在应答队列上注册一个消费者
     *
     * @return the newly created and registered consumer
     * @throws IOException if an error is encountered
     */
    protected DefaultConsumer setupConsumer() throws IOException {
        DefaultConsumer consumer = new DefaultConsumer(_channel) {
            @Override
            public void handleShutdownSignal(String consumerTag,
                                             ShutdownSignalException signal) {
                synchronized (_continuationMap) {
                    for (Entry<String, BlockingCell<Object>> entry : _continuationMap.entrySet()) {
                        entry.getValue().set(signal);
                    }
                    closed.set(true);
                }
            }

            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) {
                synchronized (_continuationMap) {
                    String replyId = properties.getCorrelationId();
                    BlockingCell<Object> blocker = _continuationMap.remove(replyId); // 每个replyId对应一个BlockingCell（存放应答信息）
                    if (blocker == null) {
                        // Entry should have been removed if request timed out,
                        // log a warning nevertheless.
                        LOGGER.warn("No outstanding request for correlation ID {}", replyId);
                    } else {
                        blocker.set(new Response(consumerTag, envelope, properties, body)); // 服务端的应答消息设置入BlockingCell，由客户端的主线程从中获取
                    }
                }
            }
        };
        // 订阅特定的队列消息（类比请求应答）
        String s = _channel.basicConsume(_replyTo, true, consumer);// replyTo就是一个队列：amq.rabbitmq.reply-to
        System.out.println("consumerTag="+s);
        return consumer;
    }

    public void publish(AMQP.BasicProperties props, byte[] message)
            throws IOException {
        _channel.basicPublish(_exchange, _routingKey, _useMandatory, props, message); // 消息发送到服务端订阅的队列中
    }

    public Response doCall(AMQP.BasicProperties props, byte[] message)
            throws IOException, TimeoutException {
        return doCall(props, message, _timeout);
    }

    public Response doCall(AMQP.BasicProperties props, byte[] message, int timeout)
            throws IOException, ShutdownSignalException, TimeoutException {
        checkNotClosed();
        BlockingCell<Object> k = new BlockingCell<Object>(); // 每个请求都有对应一个BlockingCell（保存服务端的应答）
        String replyId;
        synchronized (_continuationMap) {
            replyId = _correlationIdSupplier.get(); // 1
            lastCorrelationId = replyId;
            props = ((props == null) ? new AMQP.BasicProperties.Builder() : props.builder()) // #contentHeader<basic>(content-type=null, content-encoding=null, headers=null, delivery-mode=null, priority=null, correlation-id=1, reply-to=amq.rabbitmq.reply-to, expiration=null, message-id=null, timestamp=null, type=null, user-id=null, app-id=null, cluster-id=null)
                    .correlationId(replyId).replyTo(_replyTo).build();
            _continuationMap.put(replyId, k); // 发送之前先保存BlockingCell，BlockingCell用于保存服务端回复的消息，收到服务端的应答后，将消息设置入BlockingCell，客户端处理应答时再从集合移除
        }
        publish(props, message);
        Object reply;
        try {
            reply = k.uninterruptibleGet(timeout); // 客户端等待服务端的应答
        } catch (TimeoutException ex) {
            // Avoid potential leak.  This entry is no longer needed by caller.
            _continuationMap.remove(replyId);
            throw ex;
        }
        return _replyHandler.apply(reply);
    }

    public byte[] primitiveCall(AMQP.BasicProperties props, byte[] message)
            throws IOException, ShutdownSignalException, TimeoutException {
        return primitiveCall(props, message, _timeout);
    }

    public byte[] primitiveCall(AMQP.BasicProperties props, byte[] message, int timeout)
            throws IOException, ShutdownSignalException, TimeoutException {
        return doCall(props, message, timeout).getBody();
    }

    /**
     * Perform a simple byte-array-based RPC roundtrip.
     *
     * @param message the byte array request message to send
     * @return the byte array response received
     * @throws ShutdownSignalException if the connection dies during our wait
     * @throws IOException             if an error is encountered
     * @throws TimeoutException        if a response is not received within the configured timeout
     */
    public byte[] primitiveCall(byte[] message)
            throws IOException, ShutdownSignalException, TimeoutException {
        return primitiveCall(null, message);
    }

    /**
     * Perform a simple byte-array-based RPC roundtrip
     * <p>
     * Useful if you need to get at more than just the body of the message
     *
     * @param message the byte array request message to send
     * @return The response object is an envelope that contains all of the data provided to the `handleDelivery` consumer
     * @throws ShutdownSignalException if the connection dies during our wait
     * @throws IOException             if an error is encountered
     * @throws TimeoutException        if a response is not received within the configured timeout
     */
    public Response responseCall(byte[] message) throws IOException, ShutdownSignalException, TimeoutException {
        return responseCall(message, _timeout);
    }

    /**
     * Perform a simple byte-array-based RPC roundtrip
     * <p>
     * Useful if you need to get at more than just the body of the message
     *
     * @param message the byte array request message to send
     * @param timeout milliseconds before timing out on wait for response
     * @return The response object is an envelope that contains all of the data provided to the `handleDelivery` consumer
     * @throws ShutdownSignalException if the connection dies during our wait
     * @throws IOException             if an error is encountered
     * @throws TimeoutException        if a response is not received within the configured timeout
     */
    public Response responseCall(byte[] message, int timeout) throws IOException, ShutdownSignalException, TimeoutException {
        return doCall(null, message, timeout);
    }

    /**
     * Perform a simple string-based RPC roundtrip.
     *
     * @param message the string request message to send
     * @return the string response received
     * @throws ShutdownSignalException if the connection dies during our wait
     * @throws IOException             if an error is encountered
     * @throws TimeoutException        if a timeout occurs before the response is received
     */
    @SuppressWarnings("unused")
    public String stringCall(String message)
            throws IOException, ShutdownSignalException, TimeoutException {
        byte[] request;
        try {
            request = message.getBytes(StringRpcServer.STRING_ENCODING);
        } catch (IOException _e) {
            request = message.getBytes();
        }
        byte[] reply = primitiveCall(request);
        try {
            return new String(reply, StringRpcServer.STRING_ENCODING);
        } catch (IOException _e) {
            return new String(reply);
        }
    }

    /**
     * Perform an AMQP wire-protocol-table based RPC roundtrip <br><br>
     * <p>
     * There are some restrictions on the values appearing in the table: <br>
     * they must be of type {@link String}, {@link LongString}, {@link Integer}, {@link java.math.BigDecimal}, {@link Date},
     * or (recursively) a {@link Map} of the enclosing type.
     *
     * @param message the table to send
     * @return the table received
     * @throws ShutdownSignalException if the connection dies during our wait
     * @throws IOException             if an error is encountered
     * @throws TimeoutException        if a timeout occurs before a response is received
     */
    public Map<String, Object> mapCall(Map<String, Object> message)
            throws IOException, ShutdownSignalException, TimeoutException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        MethodArgumentWriter writer = new MethodArgumentWriter(new ValueWriter(new DataOutputStream(buffer)));
        writer.writeTable(message);
        writer.flush();
        byte[] reply = primitiveCall(buffer.toByteArray());
        MethodArgumentReader reader =
                new MethodArgumentReader(new ValueReader(new DataInputStream(new ByteArrayInputStream(reply))));
        return reader.readTable();
    }

    /**
     * Perform an AMQP wire-protocol-table based RPC roundtrip, first
     * constructing the table from an array of alternating keys (in
     * even-numbered elements, starting at zero) and values (in
     * odd-numbered elements, starting at one) <br>
     * Restrictions on value arguments apply as in {@link RpcClient#mapCall(Map)}.
     *
     * @param keyValuePairs alternating {key, value, key, value, ...} data to send
     * @return the table received
     * @throws ShutdownSignalException if the connection dies during our wait
     * @throws IOException             if an error is encountered
     * @throws TimeoutException        if a timeout occurs before a response is received
     */
    public Map<String, Object> mapCall(Object[] keyValuePairs)
            throws IOException, ShutdownSignalException, TimeoutException {
        Map<String, Object> message = new HashMap<String, Object>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            message.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return mapCall(message);
    }

    /**
     * Retrieve the channel.
     *
     * @return the channel to which this client is connected
     */
    public Channel getChannel() {
        return _channel;
    }

    /**
     * Retrieve the exchange.
     *
     * @return the exchange to which this client is connected
     */
    public String getExchange() {
        return _exchange;
    }

    /**
     * Retrieve the routing key.
     *
     * @return the routing key for messages to this client
     */
    public String getRoutingKey() {
        return _routingKey;
    }

    /**
     * Retrieve the continuation map.
     *
     * @return the map of objects to blocking cells for this client
     */
    public Map<String, BlockingCell<Object>> getContinuationMap() {
        return _continuationMap;
    }

    /**
     * Retrieve the last correlation id used.
     * <p>
     * Note as of 5.9.0, correlation IDs may not always be integers
     * (by default, they are).
     * This method will try to parse the last correlation ID string
     * as an integer, so this may result in {@link NumberFormatException}
     * if the correlation ID supplier provided by
     * {@link RpcClientParams#correlationIdSupplier(Supplier)}
     * does not generate appropriate IDs.
     *
     * @return the most recently used correlation id
     * @see RpcClientParams#correlationIdSupplier(Supplier)
     */
    public int getCorrelationId() {
        return Integer.valueOf(this.lastCorrelationId);
    }

    /**
     * Retrieve the consumer.
     *
     * @return an interface to the client's consumer object
     */
    public Consumer getConsumer() {
        return _consumer;
    }

    /**
     * The response object is an envelope that contains all of the data provided to the `handleDelivery` consumer
     */
    public static class Response {
        protected String consumerTag;
        protected Envelope envelope;
        protected AMQP.BasicProperties properties;
        protected byte[] body;

        public Response() {
        }

        public Response(
                final String consumerTag, final Envelope envelope, final AMQP.BasicProperties properties,
                final byte[] body) {
            this.consumerTag = consumerTag;
            this.envelope = envelope;
            this.properties = properties;
            this.body = body;
        }

        public String getConsumerTag() {
            return consumerTag;
        }

        public Envelope getEnvelope() {
            return envelope;
        }

        public AMQP.BasicProperties getProperties() {
            return properties;
        }

        public byte[] getBody() {
            return body;
        }
    }

    /**
     * Creates generation IDs as a sequence of integers.
     *
     * @return
     * @see RpcClientParams#correlationIdSupplier(Supplier)
     * @since 5.9.0
     */
    public static Supplier<String> incrementingCorrelationIdSupplier() {
        return incrementingCorrelationIdSupplier("");
    }

    /**
     * Creates generation IDs as a sequence of integers, with the provided prefix.
     *
     * @param prefix
     * @return
     * @see RpcClientParams#correlationIdSupplier(Supplier)
     * @since 5.9.0
     */
    public static Supplier<String> incrementingCorrelationIdSupplier(String prefix) {
        return new IncrementingCorrelationIdSupplier(prefix);
    }

    /**
     * @since 5.9.0
     */
    private static class IncrementingCorrelationIdSupplier implements Supplier<String> {

        private final String prefix;
        private int correlationId;

        public IncrementingCorrelationIdSupplier(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public String get() {
            return prefix + ++correlationId;
        }

    }
}

