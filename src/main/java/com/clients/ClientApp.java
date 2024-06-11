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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

        System.out.println(executor);
        
        // Read file for performing operation
        String directoryPath = "/home/ks/eclipse-workspace/benchmark/benchmark-nosql/data/ShopingCart/project_files/xact_files/";
        int fileLength =  ca.countFiles(directoryPath);
        
        for(int i=0; i<fileLength; i++) {
            String template = "/home/ks/eclipse-workspace/benchmark/benchmark-nosql/data/ShopingCart/project_files/xact_files/%d.txt";
            int fileNumber = i+1;
            
            File file = new File(String.format(template, i, transactionFileNumber));
            
            if (file.exists()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String inputLine;
                    while ((inputLine = reader.readLine()) != null) {
                        System.out.println("Task Started");
                        executor.submit(new Task(i, fileNumber, inputLine, reader));
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
                        sc.doTransaction(thread, taskId, fileNumber, inputLine, reader);
                        System.out.println("doTransaction result: ");
                    } catch (Exception e) {
                        System.out.println("Exception during transaction: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("inputLine is null - thread: " + thread);
                }
            }

            try {
                Thread.sleep(10); // Simulating 10 milliseconds of workload
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Task completed
            System.out.println("Task " + taskId + " completed.");
        }
    }

}


