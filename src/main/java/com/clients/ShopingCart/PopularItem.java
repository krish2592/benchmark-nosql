package com.clients.ShopingCart;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;

public class PopularItem {
    private CqlSession session;

    public PopularItem(DBClient client) {
        session = client.getSession();
        
    }

    public void findItem(int wId, int dId, int l) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findItem'");
    }
}
