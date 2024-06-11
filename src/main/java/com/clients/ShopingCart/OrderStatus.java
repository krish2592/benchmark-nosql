package com.clients.ShopingCart;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;

public class OrderStatus {
    private CqlSession session;

    public OrderStatus(DBClient client) {
        session = client.getSession();
        
    }

    public void getOrderStatus(int wId, int dId, int cId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getOrderStatus'");
    }
}
