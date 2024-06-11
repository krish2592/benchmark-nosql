package com.clients.ShopingCart;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;

public class Delivery {
    private CqlSession session;

    public Delivery(DBClient client) {
        session = client.getSession();
        
    }

    public void executeQuery(int wId, int carrierId){

    }
}
