package com.clients.ShopingCart;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;


public class NewOrder {
    private CqlSession session;
    private PreparedStatement warehouseQuery;
    private PreparedStatement districtQuery;
    private PreparedStatement customerQuery;
    private PreparedStatement createOrderQuery;
    com.datastax.oss.driver.api.core.cql.ResultSet resultSet; 
    
    public NewOrder(DBClient client) {
        session = client.getSession();

        String whQuery = "select w_tax from warehouses where w_id = ?;";
        String distQuery = "select d_next_o_id, d_tax from districts where d_w_id = ? and d_id = ?;"; 
        String custQuery = "select c_last, c_credit, c_discount from customers where c_w_id = ? and c_d_id = ? and c_id = ?;";
        String orderQuery = "INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d, o_ols) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?);";

        warehouseQuery = session.prepare(whQuery);
        districtQuery = session.prepare(distQuery);
        customerQuery = session.prepare(custQuery);
        createOrderQuery = session.prepare(orderQuery);
    }

    
     public void createOrder(int w_id, int d_id, int c_id, int num_items,
                            int[] item_number, int[] supplier_warehouse, int[] quantity) {

     
        // retrieve next available order number.
        System.out.println("Execute query");
        BoundStatement boundStatement = districtQuery.bind(w_id, d_id, c_id);
        resultSet =  session.execute(boundStatement);
        Row districtRow =  resultSet.one();

        int orderNo = districtRow.getInt("d_next_o_id");
        System.out.println(orderNo);
        
        // district tax
        float district_tax = districtRow.getFloat("d_tax");

        // get warehouse tax rate
        resultSet = session.execute(warehouseQuery.bind(w_id));
        Row warehouseRow = resultSet.one();
        float warehouse_tax = warehouseRow.getFloat("w_tax");

        // get customer info
        resultSet = session.execute(customerQuery.bind(w_id, d_id, c_id));
        Row customerRow = resultSet.one();

        float discount = customerRow.getFloat("c_discount");
        String lastName = customerRow.getString("c_last");
        String credit = customerRow.getString("c_credit");

        System.out.println(String.format("User %s, %s, %.2f", lastName, credit, discount));
        System.out.println(String.format("Warehouse Tax: %.2f, District tax: %.2f", warehouse_tax, district_tax));

        session.execute(String.format("update districts set d_next_o_id = %d where d_w_id = %d and d_id = %d;",
                orderNo + 1, w_id, d_id));

        // check if local order
        int isAllLocal = 1;
        for (int orderId : supplier_warehouse) {
            if (orderId != w_id) {
                isAllLocal = 0;
                break;
            }
        }

        // insert this order
        Date entryDate = new Date();
        System.out.println(String.format("Order number: %d, %s", orderNo, entryDate));

        float totalAmount = (float)0.0;
        int item, warehouse, request_quantity, s_order_cnt, s_remote_cnt, s_quantity, adjusted_quantity;
        String district_info, name;
        float s_ytd;
        float price, item_amount;
        Row resultRow;
        ArrayList<String> outputInfo = new ArrayList<String>();


    }

    public static void main(String[] args) {
        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        System.out.println("Connected to datacenter1");

        NewOrder transaction = new NewOrder(client);
        int[] warehouses = new int[] {1, 2};
        int[] items = new int[] {1, 2};
        int[] quantity = new int[] {4, 1};
        transaction.createOrder(1, 7, 7, 2, items, warehouses, quantity);
        transaction.createOrder(2, 7, 13, 2, items, warehouses, quantity);
        client.close();
    }
}
