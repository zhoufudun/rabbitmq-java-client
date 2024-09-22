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

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.test.BrokerTestCase;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class Heartbeat extends BrokerTestCase {

    @Override
    protected ConnectionFactory newConnectionFactory() {
        ConnectionFactory cf = super.newConnectionFactory();
        cf.setRequestedHeartbeat(1); // 实际上客户端发送心跳的间隔1/2s一次：com.rabbitmq.client.impl.AMQConnection.setHeartbeat
        return cf;
    }

    @Test
    public void heartbeat() throws InterruptedException {
        assertEquals(1, connection.getHeartbeat());
        Thread.sleep(3100);
        assertTrue(connection.isOpen());
        ((AutorecoveringConnection) connection).getDelegate().setHeartbeat(0); // 0 表示关闭心跳，会取消心跳任务
        assertEquals(0, connection.getHeartbeat());
        Thread.sleep(3100); // 等待服务端关闭连接
        assertFalse(connection.isOpen());

    }

}
