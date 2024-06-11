package com.clients.ShopingCart;

import java.io.BufferedReader;
import java.util.UUID;
import com.clients.DBClient;


public class ShopingCartClient {
    
    /**
     * @param thread
     * @param taskId
     * @param fileNumber
     * @param queryString
     */
    public void doTransaction(int thread, UUID taskId, int fileNumber, String inputLine, BufferedReader reader ) {
        // Initialize connection
        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        System.out.println("Connected to datacenter1");


        // Create session
        NewOrder newOrder = new NewOrder(client);
        // OrderStatus orderStatus = new OrderStatus(client);
        // Payment payment = new Payment(client);
        // PopularItem popularItem = new PopularItem(client);
        // Delivery delivery = new Delivery(client);
        // StockLevel stockLevel = new StockLevel(client);

        try {
                String[] lineParams = inputLine.split(",");
                long[] timings = new long[6];
                int[] transactionCounts = new int[6];
                long startTime;


                if (inputLine.charAt(0) == 'N') {
                    int cId = Integer.parseInt(lineParams[1]);
                    int wId = Integer.parseInt(lineParams[2]);
                    int dId = Integer.parseInt(lineParams[3]);
                    int numItems = Integer.parseInt(lineParams[4]);
                    int[] itemNumbers = new int[numItems];
                    int[] supplierWarehouse = new int[numItems];
                    int[] quantity = new int[numItems];

                    String newLine;
                    String[] newParams;
                    for (int j = 0; j < numItems; j++) {
                        newLine = reader.readLine();
                        newParams = newLine.split(",");
                        itemNumbers[j] = Integer.parseInt(newParams[0]);
                        supplierWarehouse[j] = Integer.parseInt(newParams[1]);
                        quantity[j] = Integer.parseInt(newParams[2]);
                    }

                    try {
                        startTime = System.nanoTime();
                        newOrder.createOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
                        timings[0] = timings[0] + (System.nanoTime() - startTime);
                        transactionCounts[0] = transactionCounts[0] + 1;
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                } 



                //else if (inputLine.charAt(0) == 'P') {
                //     int wId = Integer.parseInt(lineParams[1]);
                //     int dId = Integer.parseInt(lineParams[2]);
                //     int cId = Integer.parseInt(lineParams[3]);
                //     float paymentDetail = Float.parseFloat(lineParams[4]);

                //     try {
                //         startTime = System.nanoTime();
                //         payment.processPayment(wId, dId, cId, paymentDetail);
                //         timings[1] = timings[1] + (System.nanoTime() - startTime);
                //         transactionCounts[1] = transactionCounts[1] + 1;
                //     } catch (Exception e) {
                //         System.err.println(e.getMessage());
                //     }
                // } else if (inputLine.charAt(0) == 'D') {
                //     int wId = Integer.parseInt(lineParams[1]);
                //     int carrierId = Integer.parseInt(lineParams[2]);

                //     try {
                //         startTime = System.nanoTime();
                //         delivery.executeQuery(wId, carrierId);
                //         timings[2] = timings[2] + (System.nanoTime() - startTime);
                //         transactionCounts[2] = transactionCounts[2] + 1;
                //     } catch (Exception e) {
                //         System.err.println(e.getMessage());
                //     }
                // } else if (inputLine.charAt(0) == 'O') { // Order Status
                //     int wId = Integer.parseInt(lineParams[1]);
                //     int dId = Integer.parseInt(lineParams[2]);
                //     int cId = Integer.parseInt(lineParams[3]);

                //     try {
                //         startTime = System.nanoTime();
                //         orderStatus.getOrderStatus(wId, dId, cId);
                //         timings[3] = timings[3] + (System.nanoTime() - startTime);
                //         transactionCounts[3] = transactionCounts[3] + 1;
                //     } catch (Exception e) {
                //         System.err.println(e.getMessage());
                //     }
                // } else if (inputLine.charAt(0) == 'S') {
                //     int wId = Integer.parseInt(lineParams[1]);
                //     int dId = Integer.parseInt(lineParams[2]);
                //     int T = Integer.parseInt(lineParams[3]);
                //     int L = Integer.parseInt(lineParams[4]);

                //     try {
                //         startTime = System.nanoTime();
                //         stockLevel.executeQuery(wId, dId, T, L);
                //         timings[4] = timings[4] + (System.nanoTime() - startTime);
                //         transactionCounts[4] = transactionCounts[4] + 1;
                //     } catch (Exception e) {
                //         System.err.println(e.getMessage());
                //     }
                // } else if (inputLine.charAt(0) == 'I') {
                //     int wId = Integer.parseInt(lineParams[1]);
                //     int dId = Integer.parseInt(lineParams[2]);
                //     int L = Integer.parseInt(lineParams[3]);

                //     try {
                //         startTime = System.nanoTime();
                //         popularItem.findItem(wId, dId, L);
                //         timings[5] = timings[5] + (System.nanoTime() - startTime);
                //         transactionCounts[5] = transactionCounts[5] + 1;
                //     } catch (Exception e) {
                //         System.err.println(e.getMessage());
                //     }
                // } 
                // else {
                //     System.err.println("\n\nSeems the way of reading of file is wrong\n\n");
                // }
                // System.out.println();


            
        } catch(Exception e) {
            e.printStackTrace();
        }

    }


}
