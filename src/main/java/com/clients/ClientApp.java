package com.clients;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import com.google.common.util.concurrent.RateLimiter;
import com.clients.ShopingCart.ShopingCartClient;

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

        RateLimiter rateLimiter = RateLimiter.create(2.0);
        // Read file for performing operation
        String directoryPath = "/home/ks/eclipse-workspace/benchmark/benchmark-nosql/data/ShopingCart/project_files/xact_files/temp";
        int fileLength =  ca.countFiles(directoryPath);
        
        for(int i=0; i<fileLength; i++) {
            String template = "/home/ks/eclipse-workspace/benchmark/benchmark-nosql/data/ShopingCart/project_files/xact_files/temp/%d.txt";
            int fileNumber = i+1;
            
            File file = new File(String.format(template, i, transactionFileNumber));

            // List<String> addQueries = new ArrayList<String>();
            // String batchSize = properties.getProperty("BATCH_SIZE");

            // if(file.exists()) {
            //     try {
            //         BufferedReader reader = new BufferedReader(new FileReader(file));
                     
            //         addQueries.add(reader.readLine());

            //     } catch (IOException e) {
            //         // TODO: handle exception
            //     }
            // }


            
            if (file.exists()) {
                try (
                    BufferedReader reader = new BufferedReader(new FileReader(file))
                ) {
                    String inputLine;
                    while ((inputLine = reader.readLine()) != null) {
                        rateLimiter.acquire();
                        if(inputLine.charAt(0) == 'N') {
                            List<String> batchItems = new ArrayList<String>();
                            int n = Integer.valueOf(inputLine.split(",")[4]);
                            for(int k=0; k<n; k++) {
                                batchItems.add(inputLine);
                                inputLine = reader.readLine();
                            }
                            executor.submit( 
                                new Task(fileNumber, batchItems));
                        } else {
                            List<String> batchItems = new ArrayList<String>();
                            batchItems.add(inputLine);
                            executor.submit(new Task(fileNumber, batchItems));
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("File " + file.getAbsolutePath() + " does not exist.");
            }
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

    public ClientApp(boolean useD8, int transactionFileNumber) {
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
        private final int fileNumber;
        private static final Lock lock = new ReentrantLock();
        private UUID transactionId;
        private ShopingCartClient sc;
        private List<String> batchItems;

        Properties properties = new Properties();

        public Task(int fileNumber, List<String> batchItems) {
            this.batchItems = batchItems;
            this.fileNumber = fileNumber;
            this.transactionId = UUID.randomUUID();
            this.sc = new ShopingCartClient();
        }

        @Override
        public void run() {
            try {
                long threadId = Thread.currentThread().getId();
                lock.lock();
                if (!batchItems.isEmpty()) {
                    try {
                        sc.doTransaction(threadId, transactionId, fileNumber, batchItems);
                    } catch (Exception e) {
                        System.out.println("Exception during transaction: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(Integer.parseInt(properties.getProperty("SLEEP_TIME_IN_MS")));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                System.out.println("completed.");
            } finally {
                lock.unlock();
            }
        }
    }

}


