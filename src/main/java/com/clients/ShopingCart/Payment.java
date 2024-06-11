package com.clients.ShopingCart;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;

public class Payment {
    private CqlSession session;

    public Payment(DBClient client) {
        session = client.getSession();
        
    }

    public void processPayment(int wId, int dId, int cId, Float payment){

    }
    
}
