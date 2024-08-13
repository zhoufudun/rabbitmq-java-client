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


package com.rabbitmq.client.impl;

import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.Basic;
import com.rabbitmq.client.AMQP.Confirm;
import com.rabbitmq.client.AMQP.Exchange;
import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.AMQP.Tx;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.observation.ObservationCollector;
import com.rabbitmq.utility.BlockingValueOrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Base class modelling an AMQ channel. Subclasses implement
 * {@link com.rabbitmq.client.Channel#close} and
 * {@link #processAsync processAsync()}, and may choose to override
 * {@link #processShutdownSignal processShutdownSignal()} and
 * {@link #rpc rpc()}.
 *
 * @see ChannelN
 * @see Connection
 */
public abstract class AMQChannel extends ShutdownNotifierComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AMQChannel.class);

    static final int NO_RPC_TIMEOUT = 0;

    /**
     * Protected; used instead of synchronizing on the channel itself,
     * so that clients can themselves use the channel to synchronize
     * on.
     */
    protected final Lock _channelLock = new ReentrantLock();
    protected final Condition _channelLockCondition = _channelLock.newCondition();

    /**
     * The connection this channel is associated with.
     */
    private final AMQConnection _connection; // RecoveryAwareAMQConnection

    /**
     * This channel's channel number.
     */
    private final int _channelNumber;

    /**
     * Command being assembled
     */
    private AMQCommand _command;

    /**
     * 当前未完成的 RPC 请求（如果有的话）。未来可能会发展为一个队列
     * The current outstanding RPC request, if any. (Could become a queue in future.)
     */
    private RpcWrapper _activeRpc = null;

    /**
     * Whether transmission of content-bearing methods should be blocked
     */
    volatile boolean _blockContent = false;

    /**
     * Timeout for RPC calls
     */
    final int _rpcTimeout;

    private final boolean _checkRpcResponseType;

    private final TrafficListener _trafficListener;
    private final int maxInboundMessageBodySize;

    private final ObservationCollector.ConnectionInfo connectionInfo;

    /**
     * Construct a channel on the given connection, with the given channel number.
     *
     * @param connection    the underlying connection for this channel
     * @param channelNumber the allocated reference number for this channel
     */
    public AMQChannel(AMQConnection connection, int channelNumber) {
        this._connection = connection;
        this._channelNumber = channelNumber;
        if (connection.getChannelRpcTimeout() < 0) {
            throw new IllegalArgumentException("Continuation timeout on RPC calls cannot be less than 0");
        }
        this._rpcTimeout = connection.getChannelRpcTimeout();
        this._checkRpcResponseType = connection.willCheckRpcResponseType();
        this._trafficListener = connection.getTrafficListener();
        this.maxInboundMessageBodySize = connection.getMaxInboundMessageBodySize();
        this._command = new AMQCommand(this.maxInboundMessageBodySize);
        this.connectionInfo = connection.connectionInfo();
    }

    /**
     * Public API - Retrieves this channel's channel number.
     *
     * @return the channel number
     */
    public int getChannelNumber() {
        return _channelNumber;
    }

    /**
     * Private API - When the Connection receives a Frame for this
     * channel, it passes it to this method.
     *
     * @param frame the incoming frame
     * @throws IOException if an error is encountered
     */
    void handleFrame(Frame frame) throws IOException {
        AMQCommand command = _command;
        if (command.handleFrame(frame)) { // a complete command has rolled off the assembly line
            _command = new AMQCommand(this.maxInboundMessageBodySize); // prepare for the next one 重置，为下一个请求做准备
            handleCompleteInboundCommand(command); // 处理完整的入站命令
        }
    }

    /**
     * Placeholder until we address bug 15786 (implementing a proper exception hierarchy).
     * In the meantime, this at least won't throw away any information from the wrapped exception.
     *
     * @param ex the exception to wrap
     * @return the wrapped exception
     */
    public static IOException wrap(ShutdownSignalException ex) {
        return wrap(ex, null);
    }

    public static IOException wrap(ShutdownSignalException ex, String message) {
        return new IOException(message, ex);
    }

    /**
     * Placeholder until we address bug 15786 (implementing a proper exception hierarchy).
     */
    public AMQCommand exnWrappingRpc(Method m)
            throws IOException {
        try {
            return privateRpc(m);
        } catch (AlreadyClosedException ace) {
            // Do not wrap it since it means that connection/channel
            // was closed in some action in the past
            throw ace;
        } catch (ShutdownSignalException ex) {
            throw wrap(ex);
        }
    }

    CompletableFuture<Command> exnWrappingAsyncRpc(Method m)
            throws IOException {
        try {
            return privateAsyncRpc(m);
        } catch (AlreadyClosedException ace) {
            // Do not wrap it since it means that connection/channel
            // was closed in some action in the past
            throw ace;
        } catch (ShutdownSignalException ex) {
            throw wrap(ex);
        }
    }

    /**
     * Private API - handle a command which has been assembled
     *
     * @param command the incoming command
     * @throws IOException if there's any problem
     * @throws IOException when operation is interrupted by an I/O exception
     */
    public void handleCompleteInboundCommand(AMQCommand command) throws IOException {
        // First, offer the command to the asynchronous-command
        // handling mechanism, which gets to act as a filter on the
        // incoming command stream.  If processAsync() returns true,
        // the command has been dealt with by the filter and so should
        // not be processed further.  It will return true for
        // asynchronous commands (deliveries/returns/other events),
        // and false for commands that should be passed on to some
        // waiting RPC continuation. 首先，将命令提供给异步命令处理机制，该机制充当传入命令流的过滤器。如果 processAsync() 返回 true，表示该命令已经被过滤器处理过，因此不应进一步处理。对于异步命令（交付/返回/其他事件），它将返回 true；而对于应传递给某个等待中的 RPC 继续处理的命令，它将返回 false。
        this._trafficListener.read(command);
        if (!processAsync(command)) { // 对于应传递给某个等待中的 RPC 继续处理的命令，它将返回 false
            // The filter decided not to handle/consume the command,
            // so it must be a response to an earlier RPC.
            // 走到这里说明：过滤器决定不处理/消费该命令，因此它一定是对先前RPC的响应
            if (_checkRpcResponseType) {
                _channelLock.lock();
                try { // 在调用 nextOutstandingRpc() 之前，检查这个回复命令是否是针对当前等待的请求。
                    // check if this reply command is intended for the current waiting request before calling nextOutstandingRpc()
                    if (_activeRpc != null && !_activeRpc.canHandleReply(command)) {
                        // this reply command is not intended for the current waiting request
                        // most likely a previous request timed out and this command is the reply for that.
                        // Throw this reply command away so we don't stop the current request from waiting for its reply
                        // 这个回复命令可能并非针对当前等待的请求，很可能是之前的请求超时了，而这个命令是该请求的回复。丢弃这个回复命令，以免阻止当前请求继续等待它的回复。
                        return;
                    }
                } finally {
                    _channelLock.unlock();
                }
            }
            final RpcWrapper nextOutstandingRpc = nextOutstandingRpc(); // 获取当前待处理完成的RPC请求（等待应答）
            // the outstanding RPC can be null when calling Channel#asyncRpc
            if (nextOutstandingRpc != null) {
                nextOutstandingRpc.complete(command); // 将服务端返回的结果封装到一个对象中，并且唤醒正在等待此结果的RPC请求线程，唤醒它之后，它可以获取这个结果。
                markRpcFinished();
            }
        }
    }

    public void enqueueRpc(RpcContinuation k) { // SimpleBlockingRpcContinuation
        doEnqueueRpc(() -> new RpcContinuationRpcWrapper(k));
    }

    private void enqueueAsyncRpc(Method method, CompletableFuture<Command> future) {
        doEnqueueRpc(() -> new CompletableFutureRpcWrapper(method, future));
    }

    private void doEnqueueRpc(Supplier<RpcWrapper> rpcWrapperSupplier) {
        _channelLock.lock();
        try {
            boolean waitClearedInterruptStatus = false;
            while (_activeRpc != null) { // 将新的请求准备加入时，发现已经存在一个正在等待处理的请求，此时需要等待上一个请求被处理，上一个请求被处理后_activeRpc会被置为null，并且唤醒_channelLockCondition.await()，这时本次的请求才能存入
                try {
                    _channelLockCondition.await(); //
                } catch (InterruptedException e) { //NOSONAR
                    waitClearedInterruptStatus = true;
                    // No Sonar: we re-interrupt the thread later
                }
            }
            if (waitClearedInterruptStatus) {
                Thread.currentThread().interrupt();
            }
            _activeRpc = rpcWrapperSupplier.get(); // RpcContinuationRpcWrapper： 设置一下当前激活中的请求，这个请求是需要再MainLoop线程中收到服务端的消息后，处理服务端的消息，然后再处理本次激活的_activeRpc，处理完后清空_activeRpc，在唤醒其他正在调用doEnqueueRpc的线程
        } finally {
            _channelLock.unlock();
        }
    }

    boolean isOutstandingRpc() {
        _channelLock.lock();
        try {
            return (_activeRpc != null);
        } finally {
            _channelLock.unlock();
        }
    }

    public RpcWrapper nextOutstandingRpc() {
        _channelLock.lock();
        try {
            RpcWrapper result = _activeRpc; // 临时保存上一个请求
            _activeRpc = null; // 清空上一个请求
            _channelLockCondition.signalAll(); // 唤醒其他等待_channelLockCondition的线程：比如doEnqueueRpc中的 _channelLockCondition.await();
            return result;
        } finally {
            _channelLock.unlock();
        }
    }

    protected void markRpcFinished() {
        // no-op
    }

    private void ensureIsOpen()
            throws AlreadyClosedException {
        if (!isOpen()) {
            throw new AlreadyClosedException(getCloseReason());
        }
    }

    /**
     * Protected API - sends a {@link Method} to the broker and waits for the
     * next in-bound Command from the broker: only for use from
     * non-connection-MainLoop threads!
     */
    public AMQCommand rpc(Method m)
            throws IOException, ShutdownSignalException {
        return privateRpc(m);
    }

    public AMQCommand rpc(Method m, int timeout)
            throws IOException, ShutdownSignalException, TimeoutException {
        return privateRpc(m, timeout);
    }

    private AMQCommand privateRpc(Method m)
            throws IOException, ShutdownSignalException {
        SimpleBlockingRpcContinuation k = new SimpleBlockingRpcContinuation(m);
        rpc(m, k);
        // At this point, the request method has been sent, and we
        // should wait for the reply to arrive.
        //
        // Calling getReply() on the continuation puts us to sleep
        // until the connection's reader-thread throws the reply over
        // the fence or the RPC times out (if enabled)
        if (_rpcTimeout == NO_RPC_TIMEOUT) {
            return k.getReply();
        } else {
            try {
                return k.getReply(_rpcTimeout);
            } catch (TimeoutException e) {
                throw wrapTimeoutException(m, e);
            }
        }
    }

    private void cleanRpcChannelState() {
        try {
            // clean RPC channel state
            nextOutstandingRpc();
            markRpcFinished();
        } catch (Exception ex) {
            LOGGER.warn("Error while cleaning timed out channel RPC: {}", ex.getMessage());
        }
    }

    /**
     * Cleans RPC channel state after a timeout and wraps the TimeoutException in a ChannelContinuationTimeoutException
     */
    ChannelContinuationTimeoutException wrapTimeoutException(final Method m, final TimeoutException e) {
        cleanRpcChannelState();
        return new ChannelContinuationTimeoutException(e, this, this._channelNumber, m);
    }

    private CompletableFuture<Command> privateAsyncRpc(Method m)
            throws IOException, ShutdownSignalException {
        CompletableFuture<Command> future = new CompletableFuture<>();
        asyncRpc(m, future);
        return future;
    }

    private AMQCommand privateRpc(Method m, int timeout)
            throws IOException, ShutdownSignalException, TimeoutException {
        SimpleBlockingRpcContinuation k = new SimpleBlockingRpcContinuation(m);
        rpc(m, k);

        try {
            return k.getReply(timeout); // 等待服务端的应答
        } catch (TimeoutException e) {
            cleanRpcChannelState();
            throw e;
        }
    }

    public void rpc(Method m, RpcContinuation k)
            throws IOException {
        _channelLock.lock();
        try {
            ensureIsOpen();
            quiescingRpc(m, k);
        } finally {
            _channelLock.unlock();
        }
    }

    void quiescingRpc(Method m, RpcContinuation k)
            throws IOException {
        _channelLock.lock();
        try {
            enqueueRpc(k); // AMQChannel： 将等待应答的请求保存起来，再将应答消息异步发送出去，服务端回复消息，客户端接受消息，唤醒等待应答的线程。
            quiescingTransmit(m);
        } finally {
            _channelLock.unlock();
        }
    }

    private void asyncRpc(Method m, CompletableFuture<Command> future)
            throws IOException {
        _channelLock.lock();
        try {
            ensureIsOpen();
            quiescingAsyncRpc(m, future);
        } finally {
            _channelLock.unlock();
        }
    }

    private void quiescingAsyncRpc(Method m, CompletableFuture<Command> future)
            throws IOException {
        _channelLock.lock();
        try {
            enqueueAsyncRpc(m, future);
            quiescingTransmit(m);
        } finally {
            _channelLock.unlock();
        }
    }

    /**
     * Protected API - called by nextCommand to check possibly handle an incoming Command before it is returned to the caller of nextCommand. If this method
     * returns true, the command is considered handled and is not passed back to nextCommand's caller; if it returns false, nextCommand returns the command as
     * usual. This is used in subclasses to implement handling of Basic.Return and Basic.Deliver messages, as well as Channel.Close and Connection.Close.
     *
     * @param command the command to handle asynchronously
     * @return true if we handled the command; otherwise the caller should consider it "unhandled"
     */
    public abstract boolean processAsync(Command command) throws IOException;

    @Override
    public String toString() {
        return "AMQChannel(" + _connection + "," + _channelNumber + ")";
    }

    /**
     * Protected API - respond, in the driver thread, to a {@link ShutdownSignalException}.
     *
     * @param signal       the signal to handle
     * @param ignoreClosed the flag indicating whether to ignore the AlreadyClosedException
     *                     thrown when the channel is already closed
     * @param notifyRpc    the flag indicating whether any remaining rpc continuation should be
     *                     notified with the given signal
     */
    public void processShutdownSignal(ShutdownSignalException signal,
                                      boolean ignoreClosed,
                                      boolean notifyRpc) {
        try {
            _channelLock.lock();
            try {
                if (!setShutdownCauseIfOpen(signal)) {
                    if (!ignoreClosed)
                        throw new AlreadyClosedException(getCloseReason());
                }

                _channelLockCondition.signalAll();
            } finally {
                _channelLock.unlock();
            }
        } finally {
            if (notifyRpc)
                notifyOutstandingRpc(signal);
        }
    }

    void notifyOutstandingRpc(ShutdownSignalException signal) {
        RpcWrapper k = nextOutstandingRpc();
        if (k != null) {
            k.shutdown(signal);
        }
    }

    public void transmit(Method m) throws IOException {
        _channelLock.lock();
        try {
            transmit(new AMQCommand(m));
        } finally {
            _channelLock.unlock();
        }
    }

    public void transmit(AMQCommand c) throws IOException {
        _channelLock.lock();
        try {
            ensureIsOpen();
            quiescingTransmit(c);
        } finally {
            _channelLock.unlock();
        }
    }

    public void quiescingTransmit(Method m) throws IOException {
        _channelLock.lock();
        try {
            quiescingTransmit(new AMQCommand(m)); // 给服务端的消息封装起来，异步发送
        } finally {
            _channelLock.unlock();
        }
    }

    public void quiescingTransmit(AMQCommand c) throws IOException {
        _channelLock.lock();
        try {
            if (c.getMethod().hasContent()) { // 如果当前要传输的指令有内容，需要加锁等待其他的发送完成
                while (_blockContent) {
                    try { // ？
                        _channelLockCondition.await();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }

                    // This is to catch a situation when the thread wakes up during
                    // shutdown. Currently, no command that has content is allowed
                    // to send anything in a closing state.
                    ensureIsOpen();
                }
            }
            this._trafficListener.write(c);   // 流控
            c.transmit(this);
        } finally {
            _channelLock.unlock();
        }
    }

    public AMQConnection getConnection() {
        return _connection;
    }

    public interface RpcContinuation {
        void handleCommand(AMQCommand command);

        /**
         * @return true if the reply command can be handled for this request
         */
        boolean canHandleReply(AMQCommand command);

        void handleShutdownSignal(ShutdownSignalException signal);
    }

    public static abstract class BlockingRpcContinuation<T> implements RpcContinuation {
        final BlockingValueOrException<T, ShutdownSignalException> _blocker =
                new BlockingValueOrException<>();

        protected final Method request;

        BlockingRpcContinuation() {
            request = null;
        }

        BlockingRpcContinuation(final Method request) {
            this.request = request;
        }

        @Override
        public void handleCommand(AMQCommand command) {
            _blocker.setValue(transformReply(command));
        }

        @Override
        public void handleShutdownSignal(ShutdownSignalException signal) {
            _blocker.setException(signal);
        }

        public T getReply() throws ShutdownSignalException {
            return _blocker.uninterruptibleGetValue();
        }

        T getReply(int timeout) throws ShutdownSignalException, TimeoutException {
            return _blocker.uninterruptibleGetValue(timeout);
        }

        @Override
        public boolean canHandleReply(AMQCommand command) {
            return isResponseCompatibleWithRequest(request, command.getMethod());
        }

        public abstract T transformReply(AMQCommand command);

        static boolean isResponseCompatibleWithRequest(Method request, Method response) {
            // make a best effort attempt to ensure the reply was intended for this rpc request
            // Ideally each rpc request would tag an id on it that could be returned and referenced on its reply.
            // But because that would be a very large undertaking to add passively this logic at least protects against ClassCastExceptions
            if (request != null) {
                if (request instanceof Basic.Qos) {
                    return response instanceof Basic.QosOk;
                } else if (request instanceof Basic.Get) {
                    return response instanceof Basic.GetOk || response instanceof Basic.GetEmpty;
                } else if (request instanceof Basic.Consume) {
                    if (!(response instanceof Basic.ConsumeOk))
                        return false;
                    // can also check the consumer tags match here. handle case where request consumer tag is empty and server-generated.
                    final String consumerTag = ((Basic.Consume) request).getConsumerTag();
                    return consumerTag == null || consumerTag.equals("") || consumerTag.equals(((Basic.ConsumeOk) response).getConsumerTag());
                } else if (request instanceof Basic.Cancel) {
                    if (!(response instanceof Basic.CancelOk))
                        return false;
                    // can also check the consumer tags match here
                    return ((Basic.Cancel) request).getConsumerTag().equals(((Basic.CancelOk) response).getConsumerTag());
                } else if (request instanceof Basic.Recover) {
                    return response instanceof Basic.RecoverOk;
                } else if (request instanceof Exchange.Declare) {
                    return response instanceof Exchange.DeclareOk;
                } else if (request instanceof Exchange.Delete) {
                    return response instanceof Exchange.DeleteOk;
                } else if (request instanceof Exchange.Bind) {
                    return response instanceof Exchange.BindOk;
                } else if (request instanceof Exchange.Unbind) {
                    return response instanceof Exchange.UnbindOk;
                } else if (request instanceof Queue.Declare) {
                    // we cannot check the queue name, as the server can strip some characters
                    // see QueueLifecycle test and https://github.com/rabbitmq/rabbitmq-server/issues/710
                    return response instanceof Queue.DeclareOk;
                } else if (request instanceof Queue.Delete) {
                    return response instanceof Queue.DeleteOk;
                } else if (request instanceof Queue.Bind) {
                    return response instanceof Queue.BindOk;
                } else if (request instanceof Queue.Unbind) {
                    return response instanceof Queue.UnbindOk;
                } else if (request instanceof Queue.Purge) {
                    return response instanceof Queue.PurgeOk;
                } else if (request instanceof Tx.Select) {
                    return response instanceof Tx.SelectOk;
                } else if (request instanceof Tx.Commit) {
                    return response instanceof Tx.CommitOk;
                } else if (request instanceof Tx.Rollback) {
                    return response instanceof Tx.RollbackOk;
                } else if (request instanceof Confirm.Select) {
                    return response instanceof Confirm.SelectOk;
                }
            }
            // for passivity default to true
            return true;
        }
    }

    public static class SimpleBlockingRpcContinuation
            extends BlockingRpcContinuation<AMQCommand> {

        SimpleBlockingRpcContinuation() {
            super();
        }

        SimpleBlockingRpcContinuation(final Method method) {
            super(method);
        }

        @Override
        public AMQCommand transformReply(AMQCommand command) { // command是服务端的应答消息
            return command;
        }
    }

    protected ObservationCollector.ConnectionInfo connectionInfo() {
        return this.connectionInfo;
    }
}
