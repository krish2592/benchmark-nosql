package com.clients;
import com.datastax.oss.driver.api.core.CqlSession;
// import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
// import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
// import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
// import com.datastax.oss.driver.api.core.metadata.Node;
// import com.datastax.oss.driver.api.core.session.SessionBuilder;

import java.net.InetSocketAddress;

public class DBClient {
    private CqlSession session;

    public void connect(String node, String string, String dataCenter) {
       

        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(node, 9042))
                .withLocalDatacenter(dataCenter) // Adjust the datacenter name
                .build();
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }
}
