package com.clients;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.io.InputStream;

public class ClientApp {
    private boolean useD8 = true;
    private int transactionFileNumber = 0;

    public static void main( String[] args ) {
        boolean useD8;
        int transactionFileNumber;
        if (args == null || args.length <= 0) {
            useD8 = true;
            transactionFileNumber = 1;
        } else {
            useD8 = (args[0].trim()).equals("D8");
            transactionFileNumber = Integer.parseInt(args[1]);
        }

        // Configuration
        Properties properties = new Properties();
        InputStream input = null; // Initialize outside try block

        try {
            input = new FileInputStream("/home/ks/eclipse-workspace/benchmark/benchmark-nosql/src/main/resources/config.properties");
            properties.load(input);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        int corePoolSize = Integer.parseInt(properties.getProperty("CONCURRENCY_LEVEL"));
        int maximumPoolSize = Integer.parseInt(properties.getProperty("CONCURRENCY_LEVEL"));
        long keepAliveTime = Integer.parseInt(properties.getProperty("KEEP_ALIVE_TIME"));
        int query_queue_capacity = Integer.parseInt(properties.getProperty("QUERY_QUEUE_CAPACITY"));

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(query_queue_capacity)
        );

        System.out.println(executor);
        
       // Submit tasks to the executor
       for (int i = 0; i < maximumPoolSize; i++) {
         executor.submit(new Task(i));
       }

        // Shutdown the executor when all tasks are completed
        executor.shutdown();

        // Runnable withdrawalTask = () -> doTransaction(1, -100.0);
        // Runnable depositTask = () -> doTransaction(1, 100.0);

        // threadPool.submit(withdrawalTask);
        // threadPool.submit(depositTask);
        
        // Client Instance creation
        ClientApp ca = new ClientApp(useD8, transactionFileNumber);
        // ca.multiprocess();
        ca.runQueries();
    }

       public ClientApp(boolean useD8, int transactionFileNumber) {
        this.useD8 = useD8;
        this.transactionFileNumber = transactionFileNumber;
    }

    public void runQueries() {
        // Initialize connection
        DBClient client = new DBClient();
        client.connect("172.21.0.2", "9042", "datacenter1");
        System.out.println("Connected to datacenter1");

        

        // Read file for performing operation
        String template = "/home/ks/eclipse-workspace/benchmark/benchmark-nosql/data/ShopingCart/project_files/xact_files/%d.txt";
        File file = new File(String.format(template, useD8 ? 0 : 40, transactionFileNumber));
        System.out.println(file);
        try {
            BufferedReader reader = new BufferedReader((new FileReader(file)));
            String inputLine;
            
            while((inputLine = reader.readLine()) != null  && (reader.readLine().length() > 0)) {
                // System.out.println(inputLine);
                String[] lineParams = inputLine.split(",");


                // PAYEMENT
                if(lineParams[0] == "P") {
                    System.out.println(lineParams[1]);
                }

                // NEW ORDER
                if(lineParams[0] == "N") {
                    System.out.println(lineParams[1]);
                }
                
                // ORDER STATUS
                if(lineParams[0] == "O") {
                    System.out.println(lineParams[1]);
                }

                // STOCK LEVEL
                if(lineParams[0] == "S") {
                    System.out.println(lineParams[1]);
                }

                // DELIVERY
                if(lineParams[0] == "D") {
                    System.out.println(lineParams[1]);
                }

                // Have to check
                if(lineParams[0] == "R") {
                    System.out.println(lineParams[1]);
                }

                // Have to check
                if(lineParams[0] == "I") {
                    System.out.println(lineParams[1]);
                }

            }
            
        } catch(IOException e) {
            e.printStackTrace();
        }
    //     // Initialize transaction
    //     NewOrder n = new NewOrder(client);
    //     OrderStatus o = new OrderStatus(client);
    //     Delivery d = new Delivery(client);
    //     StockLevel s = new StockLevel(client);
    //     Payment p = new Payment(client);
    //     PopularItem popular = new PopularItem(client);

    //     String pathTemplate = "../data/xact-spec-files/D%d-xact-files/%d.txt";
    //     long[] timings = new long[6];
    //     int[] transactionCounts = new int[6];
    //     long startTime;
    //     File file = new File(String.format(pathTemplate, useD8 ? 8 : 40, transactionFileNumber));
    //     try {
    //         BufferedReader reader = new BufferedReader(new FileReader(file));
    //         String inputLine = reader.readLine();
    //         while (inputLine != null && inputLine.length() > 0) {
    //             String[] params = inputLine.split(",");
    //             if (inputLine.charAt(0) == 'N') {
    //                 int cId = Integer.parseInt(params[1]);
    //                 int wId = Integer.parseInt(params[2]);
    //                 int dId = Integer.parseInt(params[3]);
    //                 int numItems = Integer.parseInt(params[4]);
    //                 int[] itemNumbers = new int[numItems];
    //                 int[] supplierWarehouse = new int[numItems];
    //                 int[] quantity = new int[numItems];

    //                 String newLine;
    //                 String[] newParams;
    //                 for (int j = 0; j < numItems; j++) {
    //                     newLine = reader.readLine();
    //                     newParams = newLine.split(",");
    //                     itemNumbers[j] = Integer.parseInt(newParams[0]);
    //                     supplierWarehouse[j] = Integer.parseInt(newParams[1]);
    //                     quantity[j] = Integer.parseInt(newParams[2]);
    //                 }

    //                 try {
    //                     startTime = System.nanoTime();
    //                     n.createOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
    //                     timings[0] = timings[0] + (System.nanoTime() - startTime);
    //                     transactionCounts[0] = transactionCounts[0] + 1;
    //                 } catch (Exception e) {
    //                     System.err.println(e.getMessage());
    //                 }
    //             } else if (inputLine.charAt(0) == 'P') {
    //                 int wId = Integer.parseInt(params[1]);
    //                 int dId = Integer.parseInt(params[2]);
    //                 int cId = Integer.parseInt(params[3]);
    //                 float payment = Float.parseFloat(params[4]);

    //                 try {
    //                     startTime = System.nanoTime();
    //                     p.processPayment(wId, dId, cId, payment);
    //                     timings[1] = timings[1] + (System.nanoTime() - startTime);
    //                     transactionCounts[1] = transactionCounts[1] + 1;
    //                 } catch (Exception e) {
    //                     System.err.println(e.getMessage());
    //                 }
    //             } else if (inputLine.charAt(0) == 'D') {
    //                 int wId = Integer.parseInt(params[1]);
    //                 int carrierId = Integer.parseInt(params[2]);

    //                 try {
    //                     startTime = System.nanoTime();
    //                     d.executeQuery(wId, carrierId);
    //                     timings[2] = timings[2] + (System.nanoTime() - startTime);
    //                     transactionCounts[2] = transactionCounts[2] + 1;
    //                 } catch (Exception e) {
    //                     System.err.println(e.getMessage());
    //                 }
    //             } else if (inputLine.charAt(0) == 'O') { // Order Status
    //                 int wId = Integer.parseInt(params[1]);
    //                 int dId = Integer.parseInt(params[2]);
    //                 int cId = Integer.parseInt(params[3]);

    //                 try {
    //                     startTime = System.nanoTime();
    //                     o.getOrderStatus(wId, dId, cId);
    //                     timings[3] = timings[3] + (System.nanoTime() - startTime);
    //                     transactionCounts[3] = transactionCounts[3] + 1;
    //                 } catch (Exception e) {
    //                     System.err.println(e.getMessage());
    //                 }
    //             } else if (inputLine.charAt(0) == 'S') {
    //                 int wId = Integer.parseInt(params[1]);
    //                 int dId = Integer.parseInt(params[2]);
    //                 int T = Integer.parseInt(params[3]);
    //                 int L = Integer.parseInt(params[4]);

    //                 try {
    //                     startTime = System.nanoTime();
    //                     s.executeQuery(wId, dId, T, L);
    //                     timings[4] = timings[4] + (System.nanoTime() - startTime);
    //                     transactionCounts[4] = transactionCounts[4] + 1;
    //                 } catch (Exception e) {
    //                     System.err.println(e.getMessage());
    //                 }
    //             } else if (inputLine.charAt(0) == 'I') {
    //                 int wId = Integer.parseInt(params[1]);
    //                 int dId = Integer.parseInt(params[2]);
    //                 int L = Integer.parseInt(params[3]);

    //                 try {
    //                     startTime = System.nanoTime();
    //                     popular.findItem(wId, dId, L);
    //                     timings[5] = timings[5] + (System.nanoTime() - startTime);
    //                     transactionCounts[5] = transactionCounts[5] + 1;
    //                 } catch (Exception e) {
    //                     System.err.println(e.getMessage());
    //                 }
    //             } else {
    //                 System.err.println("\n\nSeems the way of reading of file is wrong\n\n");
    //             }
    //             System.out.println(); // new line
    //             inputLine = reader.readLine();
    //         }
    //         reader.close();
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    //     client.close();

    //     float totalTiming = (float)0.0;
    //     float duration;
    //     float throughput;
    //     int totalCounts = 0;
    //     for (int i = 0; i < 6; i++) {
    //         if (transactionCounts[i] == 0) {
    //             continue;
    //         }
    //         duration = (float)timings[i] / 1000000000;
    //         throughput = (float)(transactionCounts[i]) / duration;
    //         totalTiming = totalTiming + duration;
    //         totalCounts = totalCounts + transactionCounts[i];
    //         System.err.println(String.format("Type %d: Total Transactions: %d", i, transactionCounts[i]));
    //         System.err.println(String.format("Type %d: Time used: %f s", i, duration));
    //         System.err.println(String.format("Type %d: Throughput: %f", i, throughput));
    //     }
    //     throughput = (float)totalCounts / totalTiming;
    //     System.err.println(String.format("Overall: Total Transactions: %d", totalCounts));
    //     System.err.println(String.format("Overall: Time used: %f s", totalTiming));
    //     System.err.println(String.format("Overall: Throughput: %f", throughput));
    }

    public void multiprocess() {
        // Get the number of available processors (CPU cores)
        int cores = Runtime.getRuntime().availableProcessors();

        // Create an ExecutorService with a thread pool size equal to the number of cores
        ExecutorService executorService = Executors.newFixedThreadPool(cores);

        // Submit tasks to the ExecutorService
        for (int i = 0; i < cores; i++) {
            final int taskId = i;
            executorService.submit(() -> {
                // Perform some CPU-intensive task
                System.out.println("Task " + taskId + " executed by thread " + Thread.currentThread().getName());
            });
        }

        // Shutdown the ExecutorService
        executorService.shutdown();
    }




    // private static void doTransaction(int accountId, double amount) {
    //     Connection connection = null;

    //     try {
    //         connection = DriverManager.getConnection(URL, USER, PASSWORD);
    //         connection.setAutoCommit(false);  // Start transaction

    //         // Lock the row for the specific account
    //         String selectQuery = "SELECT balance FROM accounts WHERE id = ? FOR UPDATE";
    //         try (PreparedStatement selectStmt = connection.prepareStatement(selectQuery)) {
    //             selectStmt.setInt(1, accountId);
    //             try (ResultSet rs = selectStmt.executeQuery()) {
    //                 if (rs.next()) {
    //                     double balance = rs.getDouble("balance");

    //                     // Check if the withdrawal is possible
    //                     if (amount < 0 && balance + amount < 0) {
    //                         System.out.println("Insufficient funds");
    //                     } else {
    //                         // Update balance
    //                         String updateQuery = "UPDATE accounts SET balance = ? WHERE id = ?";
    //                         try (PreparedStatement updateStmt = connection.prepareStatement(updateQuery)) {
    //                             updateStmt.setDouble(1, balance + amount);
    //                             updateStmt.setInt(2, accountId);
    //                             updateStmt.executeUpdate();
    //                         }
    //                     }
    //                 }
    //             }
    //         }

    //         connection.commit();  // Commit transaction

    //     } catch (SQLException e) {
    //         if (connection != null) {
    //             try {
    //                 connection.rollback();  // Rollback on error
    //             } catch (SQLException ex) {
    //                 ex.printStackTrace();
    //             }
    //         }
    //         e.printStackTrace();
    //     } finally {
    //         if (connection != null) {
    //             try {
    //                 connection.setAutoCommit(true);  // Reset autocommit
    //                 connection.close();  // Close connection
    //             } catch (SQLException e) {
    //                 e.printStackTrace();
    //             }
    //         }
    //     }
    // }
    

    static class Task implements Runnable {
        private final int taskId;
        
        public Task(int taskId) {
            this.taskId = taskId;
        }
        
        @Override
        public void run() {
            // Perform some task-specific operations
            System.out.println("Task " + taskId + " is running.");
            
            // Simulate some workload
            try {
                Thread.sleep(1000); // Simulating 1 second of workload
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Task completed
            System.out.println("Task " + taskId + " completed.");
        }
    }
}


