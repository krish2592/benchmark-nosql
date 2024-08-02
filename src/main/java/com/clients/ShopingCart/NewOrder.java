package com.clients.ShopingCart;

import java.time.Instant;
import java.util.ArrayList;
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
        String orderQuery = "INSERT INTO orders (o_w_id, o_d_id, o_id, o_all_local, o_c_id, o_carrier_id, o_entry_d, o_ol_cnt, o_ols) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?);";

        warehouseQuery = session.prepare(whQuery);
        districtQuery = session.prepare(distQuery);
        customerQuery = session.prepare(custQuery);
        createOrderQuery = session.prepare(orderQuery);
    }

    
     public void createOrder(int w_id, int d_id, int c_id, int num_items,
                            int[] item_number, int[] supplier_warehouse, int[] quantity) {

     
        // retrieve next available order number.
        System.out.println("Execute query");
        BoundStatement boundStatement = districtQuery.bind(w_id, d_id);
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

        // System.out.println(String.format("User %s, %s, %.2f", lastName, credit, discount));
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
        Instant entryInstant = entryDate.toInstant();
        System.out.println(String.format("Order number: %d, %s", orderNo, entryDate));

        float totalAmount = (float)0.0;
        int item, warehouse, request_quantity, s_order_cnt, s_remote_cnt, s_quantity, adjusted_quantity;
        String district_info, name;
        float s_ytd;
        float price, item_amount;
        Row resultRow;
        ArrayList<String> outputInfo = new ArrayList<String>();

        Map<Integer, UdtValue> orderLines = new HashMap<Integer, UdtValue>();

        // Collection type
        UserDefinedType orderLineType = session.getMetadata()
                                .getKeyspace(session.getKeyspace().get())
                                .flatMap(ks -> ks.getUserDefinedType("orderline"))
                                .orElseThrow(() -> new IllegalArgumentException("UDT 'orderline' not found"));
        
        
        // // System.out.println("keyspace-name   "+ session.getMetadata());
        System.out.println("keyspace-name   "+ session.getKeyspace().get());
        

        UdtValue newOrderLine;

        for (int i = 0; i < num_items; i++) {
            item = item_number[i];
            warehouse = supplier_warehouse[i];
            request_quantity = quantity[i];
            System.out.println(item + " " + warehouse+ " "+ request_quantity);
            System.out.println(item_number[i]+ "  "+ supplier_warehouse[i]+"  "+ quantity[i]);
            // get stock info
            resultSet = session.execute(String.format("select s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_%02d from stocks where s_w_id = %d and s_i_id = %d;", d_id, w_id, item));
            resultRow = resultSet.one();

            s_quantity = resultRow.getInt("s_quantity");
            s_ytd = resultRow.getFloat("s_ytd");
            s_order_cnt = resultRow.getInt("s_order_cnt");
            s_remote_cnt = resultRow.getInt("s_remote_cnt");
            district_info = resultRow.getString(4);

            // adjust quantity
            adjusted_quantity = s_quantity - request_quantity;
            adjusted_quantity = adjusted_quantity < 10 ? adjusted_quantity + 91 : adjusted_quantity;

            // check if it is a remote order
            s_remote_cnt = warehouse == w_id ? s_remote_cnt : s_remote_cnt + 1;

            // update stock
            session.execute(String.format("update stocks set s_quantity = %d, s_ytd = %.2f, s_order_cnt = %d, s_remote_cnt = %d where s_w_id = %d and s_i_id = %d;",
                    adjusted_quantity, s_ytd + (float)request_quantity, s_order_cnt + 1, s_remote_cnt, w_id, item));

            // Get price for this item
            resultSet = session.execute(String.format("select i_price, i_name from items where i_id = %d;", item));

            resultRow = resultSet.one();
            price = resultRow.getFloat("i_price");
            name = resultRow.getString("i_name");

            // calculate amount for this item
            item_amount = request_quantity * price;

            // sum up total amount
            totalAmount += item_amount;

            // create new orderline UDTValue
            newOrderLine = orderLineType.newValue()
                                .setInt("OL_I_ID", item)
                                .setInstant("ol_delivery_d", null)
                                .setFloat("ol_amount", item_amount)
                                .setInt("ol_supply_w_id", warehouse)
                                .setInt("ol_quantity", request_quantity)
                                .setString("ol_dist_info", district_info);
            orderLines.put(i + 1, newOrderLine);
 
            // add output info
            outputInfo.add(String.format("Item: %d: %s, Warehouse %d. Quantity: %d. Amount: %.2f. Stock: %d", i + 1, name, warehouse, request_quantity, item_amount, s_quantity));
            
        }
        
        BoundStatement boundStat = createOrderQuery.bind(
            w_id, d_id, orderNo, isAllLocal, c_id, null, entryInstant, num_items, orderLines
        );

        // System.out.println("Query being executed: " + boundStat.getPreparedStatement().getQuery());

        session.execute(boundStat);
    }

    public static void main(String[] args) {
        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        // // System.out.println("Connected to datacenter1");

        NewOrder transaction = new NewOrder(client);
        int[] warehouses = new int[] {1, 2};
        int[] items = new int[] {1, 2};
        int[] quantity = new int[] {4, 1};
        transaction.createOrder(1, 7, 7, 2, items, warehouses, quantity);
        transaction.createOrder(2, 7, 13, 2, items, warehouses, quantity);
        client.close();
    }
}
