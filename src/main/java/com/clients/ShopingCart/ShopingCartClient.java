package com.clients.ShopingCart;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.clients.DBClient;


public class ShopingCartClient {
    private static final Lock lock = new ReentrantLock();
    
    /**
     * @param threadId
     * @param transactionId
     * @param fileNumber
     * @param queryString
     */
    public void doTransaction(long threadId, UUID transactionId, int fileNumber, List<String> batchItems) {
        // Initialize connection
        DBClient client = new DBClient();
        client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
        // System.out.println("Connected to datacenter1");

        // Create session
        NewOrder newOrder = new NewOrder(client);
        OrderStatus orderStatus = new OrderStatus(client);
        Payment payment = new Payment(client);
        PopularItem popularItem = new PopularItem(client);
        Delivery delivery = new Delivery(client);
        StockLevel stockLevel = new StockLevel(client);

        try {   
                String inputLine = batchItems.get(0);
                String[] lineParams = inputLine.split(",");
                long[] timings = new long[6];
                int[] transactionCounts = new int[6];
                long startTime;
            
                if (inputLine.charAt(0) == 'N') {
                    lock.lock();
                    try {   
                    int cId = Integer.parseInt(lineParams[1]);
                    int wId = Integer.parseInt(lineParams[2]);
                    int dId = Integer.parseInt(lineParams[3]);
                    int numItems = Integer.parseInt(lineParams[4]);
                    int[] itemNumbers = new int[numItems];
                    int[] supplierWarehouse = new int[numItems];
                    int[] quantity = new int[numItems];

                    String newLine;
                    String[] newParams;
                    System.out.println("N:  "+inputLine + "batch: "+ batchItems.size() + "==>"+ batchItems);
                    for (int j = 1; j < batchItems.size(); j++) {
                        newLine = batchItems.get(j);
                        newParams = newLine.split(",");
                        itemNumbers[j-1] = Integer.parseInt(newParams[0]);
                        supplierWarehouse[j-1] = Integer.parseInt(newParams[1]);
                        quantity[j-1] = Integer.parseInt(newParams[2]);
                        System.out.println(itemNumbers[j-1]+ "  "+ supplierWarehouse[j-1]+"  "+ quantity[j-1]);
                    }

                    try {
                        startTime = System.nanoTime();
                        System.out.println(wId + " "+ dId + " " +cId + " " + numItems + " "+  itemNumbers[0] +" "+ supplierWarehouse[0] +" "+ quantity[0]);
                        newOrder.createOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
                        System.out.println("Order created");
                        timings[0] = timings[0] + (System.nanoTime() - startTime);
                        transactionCounts[0] = transactionCounts[0] + 1;
                    } catch (Exception e) {
                        System.err.println("Error Component: New Order: " + e);
                    }
                } finally {
                    lock.unlock();
                }

                } else if (inputLine.charAt(0) == 'P') {
                    lock.lock();
                    try {
                    int wId = Integer.parseInt(lineParams[1]);
                    int dId = Integer.parseInt(lineParams[2]);
                    int cId = Integer.parseInt(lineParams[3]);
                    float paymentDetail = Float.parseFloat(lineParams[4]);
                    try {
                        startTime = System.nanoTime();
                        payment.processPayment(wId, dId, cId, paymentDetail);
                        // System.out.println("Payment done");
                        timings[1] = timings[1] + (System.nanoTime() - startTime);
                        transactionCounts[1] = transactionCounts[1] + 1;
                        return;
                    } catch (Exception e) {
                        System.err.println("Error Component Payment: " + e.getMessage());
                    }
                } finally {
                    lock.unlock();
                }
                } else if (inputLine.charAt(0) == 'D') {
                    lock.lock();
                    try {
                    int wId = Integer.parseInt(lineParams[1]);
                    int carrierId = Integer.parseInt(lineParams[2]);

                    try {
                        startTime = System.nanoTime();
                        delivery.executeQuery(wId, carrierId);
                        // System.out.println("Delivery done");
                        timings[2] = timings[2] + (System.nanoTime() - startTime);
                        transactionCounts[2] = transactionCounts[2] + 1;
                    } catch (Exception e) {
                        System.err.println("Error Component Delivery: " + e.getMessage());
                    }
                } finally {
                    lock.unlock();
                }
                } else 
                if (inputLine.charAt(0) == 'O') { 
                    lock.lock();
                    try {
                    // Order Status
                    int wId = Integer.parseInt(lineParams[1]);
                    int dId = Integer.parseInt(lineParams[2]);
                    int cId = Integer.parseInt(lineParams[3]);

                    try {
                        startTime = System.nanoTime();
                        orderStatus.getOrderStatus(wId, dId, cId);
                        // System.out.println("Order done");
                        timings[3] = timings[3] + (System.nanoTime() - startTime);
                        transactionCounts[3] = transactionCounts[3] + 1;
                    } catch (Exception e) {
                        System.err.println("Error Component Order: " + e.getMessage());
                    }
                } finally {
                    lock.unlock();
                }
                } 
                else if (inputLine.charAt(0) == 'S') {
                    lock.lock();
                    try {
                    int wId = Integer.parseInt(lineParams[1]);
                    int dId = Integer.parseInt(lineParams[2]);
                    int T = Integer.parseInt(lineParams[3]);
                    int L = Integer.parseInt(lineParams[4]);

                    try {
                        startTime = System.nanoTime();
                        stockLevel.executeQuery(wId, dId, T, L);
                        // System.out.println("Stock");
                        timings[4] = timings[4] + (System.nanoTime() - startTime);
                        transactionCounts[4] = transactionCounts[4] + 1;
                    } catch (Exception e) {
                        System.err.println("Error Component Stock: " + e.getMessage());
                    }
                } finally {
                    lock.unlock();
                }
                } 
                else if (inputLine.charAt(0) == 'I') {
                    lock.lock();
                    try {
                    int wId = Integer.parseInt(lineParams[1]);
                    int dId = Integer.parseInt(lineParams[2]);
                    int L = Integer.parseInt(lineParams[3]);

                    try {
                        startTime = System.nanoTime();
                        popularItem.findItem(wId, dId, L);
                        // System.out.println("Popular Item");
                        timings[5] = timings[5] + (System.nanoTime() - startTime);
                        transactionCounts[5] = transactionCounts[5] + 1;
                    } catch (Exception e) {
                        System.err.println("Error Component I: " + e.getMessage());
                    }
                } finally {
                    lock.unlock();
                }
                } 
                else {
                    System.err.println("\n\nSeems the way of reading of file is wrong\n\n");
                }

            
        } catch(Exception e) {
            e.printStackTrace();
        }

    }


}
