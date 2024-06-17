package com.clients.ShopingCart;

import java.util.Map;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

public class StockLevel {
    private CqlSession session;
    private PreparedStatement selectNextOIdQuery;
    private PreparedStatement selectOlIIdQuery;
    private PreparedStatement selectSQuantityQuery;
    com.datastax.oss.driver.api.core.cql.ResultSet results;
    com.datastax.oss.driver.api.core.cql.ResultSet results1; 

    public StockLevel(DBClient client) {
        this.session = client.getSession();
        this.selectNextOIdQuery = session.prepare("SELECT d_next_o_id FROM Districts where d_w_id = ? AND d_id = ?;");
        this.selectOlIIdQuery = session.prepare("SELECT o_ols FROM Orders where o_w_id = ? AND o_d_id = ?" +
                " AND o_id < ? AND o_id >= ?;");
        this.selectSQuantityQuery = session.prepare("SELECT s_quantity FROM Stocks where s_w_id = ? AND s_i_id = ?;");
    }

     public void executeQuery(int inputWId, int inputDId, int inputT, int inputL) {
        results = session.execute(selectNextOIdQuery.bind(inputWId, inputDId));
        int N = 0, M = 0;
        for (Row row : results) {
            N = row.getInt("d_next_o_id");
            M = N - inputL;
            break;
        }

        results = session.execute(selectOlIIdQuery.bind(inputWId, inputDId, N, M));
        int olIId = 0, sum = 0;
        for (Row row : results) {
            Map<Integer, UdtValue> ols = row.getMap("o_ols", Integer.class, UdtValue.class);
            for (Integer i: ols.keySet()) {
                UdtValue ol = ols.get(i);
                olIId = ol.getInt("ol_i_id");
                results1 = session.execute(selectSQuantityQuery.bind(inputWId, olIId));
                for (Row row1 : results1) {
                    sum += (row1.getInt("s_quantity") < inputT) ? 1 : 0 ;
                }
            }
        }
        System.out.format("Counts of items: %d", sum);
    }

    public static void main(String[] args) {
        int inputWId = 1, inputDId = 1, inputT = 1000, inputL = 20;
        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        System.out.println("Connected to datacenter1");

        StockLevel s = new StockLevel(client);
        s.executeQuery(inputWId, inputDId, inputT, inputL);
        client.close();
    }
    
}
