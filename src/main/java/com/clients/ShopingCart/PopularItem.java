package com.clients.ShopingCart;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.clients.DBClient;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

public class PopularItem {
    private CqlSession session;
    private PreparedStatement selectOrder;
    private PreparedStatement selectCustomer;
    private PreparedStatement selectItem;
    private PreparedStatement selectDistrict;
    com.datastax.oss.driver.api.core.cql.ResultSet orders;
    com.datastax.oss.driver.api.core.cql.ResultSet customers;
    com.datastax.oss.driver.api.core.cql.ResultSet popularItems;
    com.datastax.oss.driver.api.core.cql.ResultSet resultSet;      

    public PopularItem(DBClient client) {
        this.session = client.getSession();
        // select orders with o_id > nextOrderID - range
        selectOrder = session.prepare("SELECT o_id, o_c_id, o_entry_d, o_ols FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_id >= ?;");
        // find customer whom placed this order
        selectCustomer = session.prepare("SELECT c_first, c_middle, c_last FROM customers WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?;");
        // find order details from orderlines
        //selectOrderLine = session.prepare("SELECT ol_i_id, ol_quantity FROM orderlines WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;");
        selectItem = session.prepare("SELECT i_name FROM items WHERE i_id = ?;");
        selectDistrict = session.prepare("SELECT d_next_o_id FROM districts WHERE d_w_id = ? AND d_id = ?;");
    }

    public void findItem(int W_ID, int D_ID, int range){
        // find next available order id in the district (D_W_ID,D_ID)
        int nextOrderID = findNextOrderID(W_ID, D_ID);

        orders = session.execute(selectOrder.bind(W_ID, D_ID, nextOrderID - range));

        HashMap<Integer, Integer> itemCount = new HashMap<Integer, Integer>();
        HashMap<Integer, String> popularItms = new HashMap<Integer, String>();

        // process each order and find most popular item for each order
        for(Row order: orders){
            int orderID = order.getInt("o_id");
            int customerID = order.getInt("o_c_id");


            customers = session.execute(selectCustomer.bind(W_ID, D_ID, customerID));
            Row customer = customers.all().get(0);

            System.out.println(String.format("Order Number: %d, Entry Date and Time: %s, Name: (%s %s %s),", orderID, order.getInstant("o_entry_d"),
                    customer.getString("c_first"), customer.getString("c_middle"), customer.getString("c_last")));

            // ResultSet orderLines = session.execute(selectOrderLine.bind(W_ID, D_ID, orderID));
            // get orderline details for this order
            Map<Integer, UdtValue> ols = order.getMap("o_ols", Integer.class, UdtValue.class);

            int popularItmID = -1;
            int popularItmCnt = 0;
            for(Integer key: ols.keySet()){
                UdtValue ol = ols.get(key);
                int quantity = ol.getInt("ol_quantity");
                int itmID = ol.getInt("ol_i_id");

                if( quantity >= popularItmCnt ){
                    popularItmCnt = quantity;
                    popularItmID = itmID;
                }

                if(!itemCount.containsKey(itmID)){
                    itemCount.put(itmID, 1);
                }else{
                    itemCount.put(itmID, itemCount.get(itmID) + 1);
                }
            }

            if (popularItmID != -1) {
                // find popular item name from items
                popularItems = session.execute(selectItem.bind(popularItmID));
                Row popularItem = popularItems.all().get(0);
                String itmName = popularItem.getString("i_name");
                System.out.println(String.format("Item Name: %s, Quantity: %d", itmName, popularItmCnt));

                // if not existing popular items add to them
                if(!popularItms.containsKey(popularItmID)){
                    popularItms.put(popularItmID,itmName);
                }
            }
        }

        for(Integer itmID : popularItms.keySet()){
            System.out.print("Item:" + popularItms.get(itmID) + itemCount.get(itmID) + "; ");
        }
    }

    private int findNextOrderID(int w_id, int d_id) {
        resultSet = session.execute(selectDistrict.bind(w_id, d_id));

        List<Row> districts = resultSet.all();
        if(districts.isEmpty()){
            return -1;
        }
        Row district = districts.get(0);
        return district.getInt("d_next_o_id");
    }

    public static void main(String[] args) {
        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        System.out.println("Connected to datacenter1");

        PopularItem transaction = new PopularItem(client);
        transaction.findItem(1, 1, 47);

        client.close();
    }
}
