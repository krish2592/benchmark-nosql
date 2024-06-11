package com.clients;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;

public class DBClient {
    private CqlSession session;

    public void connect(String node, int port, String dataCenter, String keyspaceName) {
        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(node, port))
                    .withLocalDatacenter(dataCenter)
                    .withKeyspace(keyspaceName)
                    .build();
        } catch (Exception e) {
            // Handle connection errors
            e.printStackTrace();
        }
    }

    public CqlSession getSession() {
        return session;
    }

    public void close() {
        if (session != null) {
            session.close();
        }
    }
}
