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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Command;

/**
 * AMQP 0-9-1-specific implementation of {@link Command} which accumulates
 * method, header and body from a series of frames, unless these are
 * supplied at construction time.
 * <h2>Concurrency</h2>
 * This class is thread-safe.
 */
public class AMQCommand implements Command {

    /** EMPTY_FRAME_SIZE = 8 = 1 + 2 + 4 + 1
     * <ul><li>1 byte of frame type</li>
     * <li>2 bytes of channel number</li>
     * <li>4 bytes of frame payload length</li>
     * <li>1 byte of payload trailer FRAME_END byte</li></ul>
     * See {@link #checkEmptyFrameSize}, an assertion checked at
     * startup.
     */
    public static final int EMPTY_FRAME_SIZE = 8; // 一个空贞构成

    /** The assembler for this command - synchronised on - contains all the state */
    private final CommandAssembler assembler; // #field<assembler>
    private final Lock assemblerLock = new ReentrantLock();

    AMQCommand(int maxBodyLength) {
        this(null, null, null, maxBodyLength);
    }

    /** Construct a command ready to fill in by reading frames */
    public AMQCommand() {
        this(null, null, null, Integer.MAX_VALUE);
    }

    /**
     * Construct a command with just a method, and without header or body.
     * @param method the wrapped method
     */
    public AMQCommand(com.rabbitmq.client.Method method) {
        this(method, null, null, Integer.MAX_VALUE);
    }

    /**
     * Construct a command with a specified method, header and body.
     * @param method the wrapped method
     * @param contentHeader the wrapped content header
     * @param body the message body data
     */
    public AMQCommand(com.rabbitmq.client.Method method, AMQContentHeader contentHeader, byte[] body) {
        this.assembler = new CommandAssembler((Method) method, contentHeader, body, Integer.MAX_VALUE);
    }

    /**
     * Construct a command with a specified method, header and body.
     * @param method the wrapped method
     * @param contentHeader the wrapped content header
     * @param body the message body data
     * @param maxBodyLength the maximum size for an inbound message body
     */
    public AMQCommand(com.rabbitmq.client.Method method, AMQContentHeader contentHeader, byte[] body,
                      int maxBodyLength) {
        this.assembler = new CommandAssembler((Method) method, contentHeader, body, maxBodyLength);
    }

    /** Public API - {@inheritDoc} */
    @Override
    public Method getMethod() {
        return this.assembler.getMethod();
    }

    /** Public API - {@inheritDoc} */
    @Override
    public AMQContentHeader getContentHeader() {
        return this.assembler.getContentHeader();
    }

    /** Public API - {@inheritDoc} */
    @Override
    public byte[] getContentBody() {
        return this.assembler.getContentBody();
    }

    public boolean handleFrame(Frame f) throws IOException {
        return this.assembler.handleFrame(f);
    }

    /** 将此命令通过指定的通道发送到该通道的连接上，可能会分成多个帧进行发送
     * Sends this command down the named channel on the channel's
     * connection, possibly in multiple frames.
     * @param channel the channel on which to transmit the command
     * @throws IOException if an error is encountered
     */
    public void transmit(AMQChannel channel) throws IOException {
        int channelNumber = channel.getChannelNumber();
        AMQConnection connection = channel.getConnection(); // RecoveryAwareAMQConnection

        assemblerLock.lock();
        try {
            Method m = this.assembler.getMethod();
            if (m.hasContent()) { // 如果有内容，分多次写出数据
                byte[] body = this.assembler.getContentBody();

                Frame headerFrame = this.assembler.getContentHeader().toFrame(channelNumber, body.length);

                int frameMax = connection.getFrameMax();
                boolean cappedFrameMax = frameMax > 0;
                int bodyPayloadMax = cappedFrameMax ? frameMax - EMPTY_FRAME_SIZE : body.length;

                if (cappedFrameMax && headerFrame.size() > frameMax) {
                    String msg = String.format("Content headers exceeded max frame size: %d > %d", headerFrame.size(), frameMax);
                    throw new IllegalArgumentException(msg);
                }
                connection.writeFrame(m.toFrame(channelNumber));
                connection.writeFrame(headerFrame);

                for (int offset = 0; offset < body.length; offset += bodyPayloadMax) {
                    int remaining = body.length - offset;

                    int fragmentLength = (remaining < bodyPayloadMax) ? remaining
                            : bodyPayloadMax;
                    Frame frame = Frame.fromBodyFragment(channelNumber, body,
                            offset, fragmentLength);
                    connection.writeFrame(frame);
                }
            } else {
                connection.writeFrame(m.toFrame(channelNumber));
            }
        } finally {
            assemblerLock.unlock();
        }

        connection.flush(); // 将数据从用户缓冲区发送到内核缓冲区
    }

    @Override public String toString() {
        return toString(false);
    }

    public String toString(boolean suppressBody){
        assemblerLock.lock();
        try {
            return new StringBuilder()
                .append('{')
                .append(this.assembler.getMethod())
                .append(", ")
                .append(this.assembler.getContentHeader())
                .append(", ")
                .append(contentBodyStringBuilder(
                        this.assembler.getContentBody(), suppressBody))
                .append('}').toString();
        } finally {
            assemblerLock.unlock();
        }
    }

    private static StringBuilder contentBodyStringBuilder(byte[] body, boolean suppressBody) {
        try {
            if (suppressBody) {
                return new StringBuilder().append(body.length).append(" bytes of payload");
            } else {
                return new StringBuilder().append('\"').append(new String(body, "UTF-8")).append('\"');
            }
        } catch (Exception e) {
            return new StringBuilder().append('|').append(body.length).append('|');
        }
    }

    /** Called to check internal code assumptions. */
    public static void checkPreconditions() {
        checkEmptyFrameSize();
    }

    /**
     * Since we're using a pre-computed value for EMPTY_FRAME_SIZE we
     * check this is actually correct when run against the framing
     * code in Frame.  由于我们使用预先计算的空帧大小值，因此在frame中对帧代码运行时，我们会检查这是否正确。
     */
    private static void checkEmptyFrameSize() {
        Frame f = new Frame(AMQP.FRAME_BODY, 0, new byte[0]);
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        try {
            f.writeTo(new DataOutputStream(s));
        } catch (IOException ioe) {
            throw new IllegalStateException("IOException while checking EMPTY_FRAME_SIZE");
        }
        int actualLength = s.toByteArray().length;
        if (EMPTY_FRAME_SIZE != actualLength) {
            throw new IllegalStateException("Internal error: expected EMPTY_FRAME_SIZE("
                    + EMPTY_FRAME_SIZE
                    + ") is not equal to computed value: " + actualLength);
        }
    }
}
