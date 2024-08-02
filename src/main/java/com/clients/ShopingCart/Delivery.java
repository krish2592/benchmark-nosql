package com.clients.ShopingCart;


import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

public class Delivery {
    private CqlSession session;
    private PreparedStatement selectMinOIdQuery;
    private PreparedStatement updateDeliveryDateQuery;
    private PreparedStatement selectBalanceCntQuery;
    private PreparedStatement updateBalanceCntQuery;
    com.datastax.oss.driver.api.core.cql.ResultSet results; 

    public Delivery(DBClient client) {
        this.session = client.getSession();

        this.selectMinOIdQuery = session.prepare("SELECT min(o_id) as min_o_id, o_c_id, o_ols "
                + "FROM Orders where o_w_id = ? AND o_d_id = ? AND o_carrier_id = ?;");

        this.updateDeliveryDateQuery = session.prepare("UPDATE Orders set o_carrier_id = ?, o_ols = ? where o_w_id = ?" +
                " and o_d_id = ? and o_id = ?;");

        this.selectBalanceCntQuery = session.prepare("SELECT c_balance, c_delivery_cnt FROM Customers WHERE c_w_id = ? AND c_d_id = ?"
                + " AND c_id = ?;");

        this.updateBalanceCntQuery = session.prepare("UPDATE Customers SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ?"
                + " AND c_id = ?;");
    }

   @SuppressWarnings("unused")
public void executeQuery(int inputWId, int inputCarrierId) {
        for (int i = 1; i <= 10; i++) {
            int dId = i;
            int minOId = 0;
            int cId = 0;
            float olSum = 0;
            int olQuantity, olSupplyWId, olIId;
            float olAmount;
            String olDistInfo;
            results = session.execute(selectMinOIdQuery.bind(inputWId, dId, inputCarrierId));
            Date now = new Date();
           
            Map<Integer, UdtValue> orderLines = new HashMap<Integer, UdtValue>();

            UserDefinedType orderLineType = session.getMetadata()
                                            .getKeyspace(session.getKeyspace().get())
                                            .flatMap(ks -> ks.getUserDefinedType("orderline"))
                                            .orElseThrow(() -> new IllegalArgumentException("UDT 'orderline' not found"));

            UdtValue newOrderLine;
            for (Row row: results) {
                // System.out.println(""+row);
                minOId = row.getInt("min_o_id");
                System.out.format("%d\n", minOId);
                cId = row.getInt("o_c_id");
                // System.out.println(""+cId);
                

                Map<Integer, UdtValue> ols = row.getMap("o_ols", Integer.class, UdtValue.class);
                for (Integer key: ols.keySet()) {
                    UdtValue ol = ols.get(key);
                    olIId = ol.getInt("ol_i_id");
                    olQuantity = ol.getInt("ol_quantity");
                    olDistInfo = ol.getString("ol_dist_info");
                    olSupplyWId = ol.getInt("ol_supply_w_id");
                    olAmount = ol.getFloat("ol_amount");
                    olSum += olAmount;
                    
                    newOrderLine = orderLineType.newValue()
                            .setInt("OL_I_ID", olIId)
                            .setInstant("ol_delivery_d", new Timestamp(now.getTime()).toInstant())
                            .setFloat("ol_amount", olAmount)
                            .setInt("ol_supply_w_id", olSupplyWId)
                            .setInt("ol_quantity", olQuantity)
                            .setString("ol_dist_info", olDistInfo);
                    orderLines.put(key, newOrderLine);
                }

                if(orderLines == null) {
                    throw new Error("orderLines can't be empty: ");
                }       

                BoundStatement boundStat =  updateDeliveryDateQuery.bind(inputCarrierId, orderLines, inputWId, dId, minOId);
                // // System.out.println(boundStat.getPreparedStatement().getQuery());
                // // System.out.println("Bound values: " + boundStat.getValues());
                session.execute(boundStat);
                break;
            }

            float cBalance = 0;
            int cCnt = 0;
            results = session.execute(selectBalanceCntQuery.bind(inputWId, dId, cId));
            for (Row row : results) {
                cBalance = row.getFloat("c_balance");
                cCnt = row.getInt("c_delivery_cnt");
            }
            cBalance += olSum;
            cCnt++;
            session.execute(updateBalanceCntQuery.bind(cBalance, cCnt, inputWId, dId, cId));
        }
        System.out.format("\nDone with Delivery\n\n");
    }


    public static void main(String[] args) {
        int inputWId = 1; 
        int inputCarrierId = 10;

        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        // System.out.println("Connected to datacenter1");

        Delivery d = new Delivery(client);
        d.executeQuery(inputWId, inputCarrierId);
        client.close();
    }
}
