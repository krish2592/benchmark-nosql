package com.clients.ShopingCart;

import java.util.List;
import java.util.Map;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

public class OrderStatus {
    private CqlSession session;
    private PreparedStatement customerQuery;
    private PreparedStatement orderQuery;
    com.datastax.oss.driver.api.core.cql.ResultSet resultSet; 

    public OrderStatus(DBClient client) {
        session = client.getSession();
        
        String custQuery = "select c_first, c_middle, c_last, c_balance from customers where c_w_id = ? and c_d_id = ? and c_id = ?;";
        String orderQuery = "select o_id, o_entry_d, o_carrier_id, o_ols from orders where o_w_id = ? and o_d_id = ? and o_c_id = ?;";

        this.customerQuery = session.prepare(custQuery);
        this.orderQuery = session.prepare(orderQuery);

    }

    public void getOrderStatus(int c_w_id, int c_d_id, int c_id) {
       // retrieve customer's information.
        resultSet = session.execute(customerQuery.bind(c_w_id, c_d_id, c_id));

        // should be a single value
        Row customer = resultSet.all().get(0);
        // System.out.println("Customer Info:");
        // System.out.println(String.format("Name: %s %s %s ,Balance: %.4f", customer.getString("c_first"), customer.getString("c_middle"),
         //       customer.getString("c_last"), customer.getFloat("c_balance")));

        // retrieve order information for this customer
        resultSet = session.execute(orderQuery.bind(c_w_id, c_d_id, c_id));
        List<Row> allOrders = resultSet.all();
        if (allOrders.size() == 0) {
            return;
        }
        // take the largest value as last order
        int targetIndex = 0;
        int lastOrderId = allOrders.get(0).getInt("o_id");
        // keep index for target order in the list
        for (int i=0; i<allOrders.size();i++) {
            Row row = allOrders.get(i);
            if (row.getInt("o_id") > lastOrderId) {
                targetIndex = i;
            }
        }

        Row lastOrder = allOrders.get(targetIndex);
        int orderId = lastOrder.getInt("o_id");

        // System.out.println("Last Order:");
        // System.out.println(String.format("id: %d, time: %s, carrier_id: %d", orderId,
              //  lastOrder.getInstant("o_entry_d"), lastOrder.getInt("o_carrier_id")));

        // retrieve order-line for this order.
        Map<Integer, UdtValue> ols = lastOrder.getMap("o_ols", Integer.class, UdtValue.class);
        for (Integer key: ols.keySet()) {
            UdtValue ol = ols.get(key);
            // System.out.println(String.format("%d, %d, %d, %.4f, %s ",ol.getInt("ol_i_id"), ol.getInt("ol_supply_w_id"),
                 //   ol.getInt("ol_quantity"), ol.getFloat("ol_amount"), ol.getInstant("ol_delivery_d")));
        }

    }


    public static void main(String[] args) {
        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        // System.out.println("Connected to datacenter1");
    
        OrderStatus transaction = new OrderStatus(client);
        transaction.getOrderStatus(1, 1, 1);
        transaction.getOrderStatus(3, 2, 20);
        transaction.getOrderStatus(2, 1, 1);
    
        client.close();
    }
}


