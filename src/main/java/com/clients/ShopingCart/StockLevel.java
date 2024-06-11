package com.clients.ShopingCart;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;

public class StockLevel {
    private CqlSession session;

    public StockLevel(DBClient client) {
        session = client.getSession();
        
    }

    public void executeQuery(int wId, int dId, int t, int l) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'executeQuery'");
    }
    
}
