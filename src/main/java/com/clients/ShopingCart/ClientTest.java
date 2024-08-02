package com.clients.ShopingCart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.clients.ClientApp;
import com.clients.DBClient;

import java.io.InputStream;

public class ClientTest {
    private boolean useD8 = true;
    private int transactionFileNumber = 0;
    private static final Lock lock = new ReentrantLock();

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

         // Client Instance creation
         ClientApp ca = new ClientApp(useD8, transactionFileNumber);

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
        
        // Read file for performing operation
        String directoryPath = "/home/ks/eclipse-workspace/benchmark/benchmark-nosql/data/ShopingCart/project_files/xact_files/";
        int fileLength =  ca.countFiles(directoryPath);
        
        for(int i=0; i<fileLength; i++) {
            String template = "/home/ks/eclipse-workspace/benchmark/benchmark-nosql/data/ShopingCart/project_files/xact_files/%d.txt";
            int fileNumber = i+1;
            
            File file = new File(String.format(template, i, transactionFileNumber));
            
            if (file.exists()) {

                Runnable task = () -> {

                    DBClient client = new DBClient();
                    client.connect("172.21.0.2", 9042, "datacenter1", "shop_db");
                    System.out.println("Connected to datacenter1");

                    NewOrder newOrder = new NewOrder(client);
                    OrderStatus orderStatus = new OrderStatus(client);
                    Payment payment = new Payment(client);
                    PopularItem popularItem = new PopularItem(client);
                    Delivery delivery = new Delivery(client);
                    StockLevel stockLevel = new StockLevel(client);

                    long[] timings = new long[6];
                    int[] transactionCounts = new int[6];
                    long startTime;

                    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                        String inputLine;
                        while ((inputLine = reader.readLine()) != null) {
                            String[] lineParams = inputLine.split(",");

                            
                            if (inputLine.charAt(0) == 'N') {
                                lock.lock();
                                try {                
                                
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
                                        System.out.println(cId + " "+ wId +" "+ dId + " " +  numItems);
                                        for (int j = 0; j < numItems; j++) {
                                            newLine = reader.readLine();
                                            newParams = newLine.split(",");
                                            itemNumbers[j] = Integer.parseInt(newParams[0]);
                                            supplierWarehouse[j] = Integer.parseInt(newParams[1]);
                                            quantity[j] = Integer.parseInt(newParams[2]);
                                        }

                                        try {
                                            startTime = System.nanoTime();
                                            System.out.println("StartTime"+" "+startTime);
                                            System.out.println(wId +" "+ dId +" "+ cId +" "+ numItems +" "+ itemNumbers + " "+ supplierWarehouse +" "+ quantity);
                                            newOrder.createOrder(wId, dId, cId, numItems, itemNumbers, supplierWarehouse, quantity);
                                            System.out.println("Order created");
                                            timings[0] = timings[0] + (System.nanoTime() - startTime);
                                            transactionCounts[0] = transactionCounts[0] + 1;
                                        } catch (Exception e) {
                                            System.err.println(e.getMessage());
                                        }

                                    } 
                                    
                                } finally {
                                    lock.unlock();
                                }
                            }  else if (inputLine.charAt(0) == 'P') {
                                lock.lock();
                                try {
                                    int wId = Integer.parseInt(lineParams[1]);
                                    int dId = Integer.parseInt(lineParams[2]);
                                    int cId = Integer.parseInt(lineParams[3]);
                                    float paymentDetail = Float.parseFloat(lineParams[4]);
                                    System.out.println(wId + " " + dId + " "+ cId + " " + paymentDetail);
                                    try {
                                        startTime = System.nanoTime();
                                        System.out.println("startTime: " + startTime);
                                        payment.processPayment(wId, dId, cId, paymentDetail);
                                        System.out.println("p-done");
                                        timings[1] = timings[1] + (System.nanoTime() - startTime);
                                        transactionCounts[1] = transactionCounts[1] + 1;
                                    } catch (Exception e) {
                                        System.err.println(e.getMessage());
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
                                        System.out.println("startTime: " + startTime);
                                        delivery.executeQuery(wId, carrierId);
                                        System.out.println("d-done");
                                        timings[2] = timings[2] + (System.nanoTime() - startTime);
                                        transactionCounts[2] = transactionCounts[2] + 1;
                                    } catch (Exception e) {
                                        System.err.println(e.getMessage());
                                    }
                                } finally {
                                    lock.unlock();
                                }
                    
                            } else if (inputLine.charAt(0) == 'O') { 
                                lock.lock();
                                try {
                                    // Order Status
                                    int wId = Integer.parseInt(lineParams[1]);
                                    int dId = Integer.parseInt(lineParams[2]);
                                    int cId = Integer.parseInt(lineParams[3]);

                                    try {
                                        startTime = System.nanoTime();
                                        System.out.println("startTime: " + startTime);
                                        orderStatus.getOrderStatus(wId, dId, cId);
                                        System.out.println("o-done");
                                        timings[3] = timings[3] + (System.nanoTime() - startTime);
                                        transactionCounts[3] = transactionCounts[3] + 1;
                                    } catch (Exception e) {
                                        System.err.println(e.getMessage());
                                    }
                                } finally {
                                    lock.unlock();
                                }
                    
                            } else if (inputLine.charAt(0) == 'S') {
                                lock.lock();
                                try {
                                    int wId = Integer.parseInt(lineParams[1]);
                                    int dId = Integer.parseInt(lineParams[2]);
                                    int T = Integer.parseInt(lineParams[3]);
                                    int L = Integer.parseInt(lineParams[4]);
                
                                    try {
                                        startTime = System.nanoTime();
                                        System.out.println("startTime: " + startTime);
                                        stockLevel.executeQuery(wId, dId, T, L);
                                        System.out.println("s-done");
                                        timings[4] = timings[4] + (System.nanoTime() - startTime);
                                        transactionCounts[4] = transactionCounts[4] + 1;
                                    } catch (Exception e) {
                                        System.err.println(e.getMessage());
                                    }
                                } finally {
                                    lock.unlock();
                                }
                            } else if (inputLine.charAt(0) == 'I') {
                                lock.lock();
                                try {
                                    int wId = Integer.parseInt(lineParams[1]);
                                    int dId = Integer.parseInt(lineParams[2]);
                                    int L = Integer.parseInt(lineParams[3]);
                
                                    try {
                                        startTime = System.nanoTime();
                                        System.out.println("startTime: " + startTime);
                                        popularItem.findItem(wId, dId, L);
                                        System.out.println("I-done");
                                        timings[5] = timings[5] + (System.nanoTime() - startTime);
                                        transactionCounts[5] = transactionCounts[5] + 1;
                                    } catch (Exception e) {
                                        System.err.println(e.getMessage());
                                    }
                                } finally {
                                    lock.lock();
                                }
                    
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                };    

                executor.submit(task);
           
            }

        // Shutdown the executor when all tasks are completed
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}


    public ClientTest(boolean useD8, int transactionFileNumber) {
        this.useD8 = useD8;
        this.transactionFileNumber = transactionFileNumber;
    }

    public Integer countFiles(String path) {
        File directory = new File(path);
        if(!directory.isDirectory()) {
            System.err.println("Not a directory");
            return 0;
        }
        int fileLength = directory.listFiles().length;
        return fileLength;
    }


    static class Task implements Runnable {
        private BufferedReader reader;
        private final int thread;
        private final int fileNumber;
        private final String inputLine;
        private static final Object lock = new Object();
        private UUID taskId;
        private ShopingCartClient sc;

        public Task(int thread, int fileNumber, String inputLine, BufferedReader reader) {
            this.thread = thread;
            this.reader = reader;
            this.fileNumber = fileNumber;
            this.inputLine = inputLine;
            this.taskId = UUID.randomUUID();
            this.sc = new ShopingCartClient();
        }

        @Override
        public void run() {
            synchronized (lock) {
                if (inputLine != null) {
                    System.out.println("Transaction started " + thread + " => " + taskId + " => " + fileNumber + " => " + inputLine + " => " + reader);
                    try {
                        // sc.doTransaction(thread, taskId, fileNumber, inputLine, reader);
                        System.out.println("doTransaction result: ");
                    } catch (Exception e) {
                        System.out.println("Exception during transaction: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

            try {
                Thread.sleep(20); // Simulating 10 milliseconds of workload
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Task completed
            System.out.println("Task " + taskId + " completed.");
        }
    }

}



