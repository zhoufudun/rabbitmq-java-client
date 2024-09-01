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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.test.TestUtils;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MalformedFrameException;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.impl.AMQCommand;
import com.rabbitmq.client.impl.SocketFrameHandler;

import javax.net.SocketFactory;


/**
 * read
 * Check that protocol negotiation works
 * 测试连接打开过程（指令）
 */
public class ConnectionOpen {
    @Test
    public void correctProtocolHeader() throws IOException {
        SocketFrameHandler fh = new SocketFrameHandler(SocketFactory.getDefault().createSocket("localhost", AMQP.PROTOCOL.PORT));
        fh.sendHeader();
        AMQCommand command = new AMQCommand();
        while (!command.handleFrame(fh.readFrame())) { // 一直等待消息，直到有消息
        }
        Method m = command.getMethod(); // #method<connection.start>(version-major=0, version-minor=9, server-properties={cluster_name=rabbit@WIN-20230608VMY, copyright=Copyright (c) 2007-2024 Broadcom Inc and/or its subsidiaries, product=RabbitMQ, capabilities={consumer_priorities=true, exchange_exchange_bindings=true, connection.blocked=true, authentication_failure_close=true, per_consumer_qos=true, basic.nack=true, direct_reply_to=true, publisher_confirms=true, consumer_cancel_notify=true}, information=Licensed under the MPL 2.0. Website: https://rabbitmq.com, version=3.13.2, platform=Erlang/OTP 27.0}, mechanisms=PLAIN AMQPLAIN, locales=en_US)

        assertTrue(m instanceof AMQP.Connection.Start, "First command must be Connection.start");
        AMQP.Connection.Start start = (AMQP.Connection.Start) m;
        assertTrue(start.getVersionMajor() < AMQP.PROTOCOL.MAJOR ||
                        (start.getVersionMajor() == AMQP.PROTOCOL.MAJOR &&
                                start.getVersionMinor() <= AMQP.PROTOCOL.MINOR),
                "Version in Connection.start is <= what we sent");
    }

    @Test
    public void crazyProtocolHeader() throws IOException {
        ConnectionFactory factory = TestUtils.connectionFactory();
        // keep the frame handler's socket
        Socket fhSocket = SocketFactory.getDefault().createSocket("localhost", AMQP.PROTOCOL.PORT);
        SocketFrameHandler fh = new SocketFrameHandler(fhSocket);
        fh.sendHeader(100, 3); // major, minor
        DataInputStream in = fh.getInputStream();
        // we should get a valid protocol header back
        byte[] header = new byte[4];
        in.read(header);
        // The protocol header is "AMQP" plus a version that the server
        // supports.  We can really only test for the first bit.
        assertEquals("AMQP", new String(header));
        in.read(header);
        assertEquals(in.available(), 0);
        // At this point the socket should have been closed.  We can
        // directly test for this, but since Socket.isClosed is purported to be
        // unreliable, we can also test whether trying to read more bytes
        // gives an error.
        if (!fhSocket.isClosed()) {
            fh.setTimeout(500);
            // NB the frame handler will return null if the socket times out
            try {
                fh.readFrame();
                fail("Expected socket read to fail due to socket being closed");
            } catch (MalformedFrameException mfe) {
                fail("Expected nothing, rather than a badly-formed something");
            } catch (IOException ioe) {
                System.out.println("expect result, error="+ioe);
            }
        }
    }

    @Test
    public void frameMaxLessThanFrameMinSize() throws IOException, TimeoutException {
        ConnectionFactory factory = TestUtils.connectionFactory();
        factory.setRequestedFrameMax(100);
        try {
            factory.newConnection();
        } catch (IOException ioe) {
            return;
        }
        fail("Broker should have closed the connection since our frame max < frame_min_size");
    }
}
