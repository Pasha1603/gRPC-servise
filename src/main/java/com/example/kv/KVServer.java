package com.example.kv;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class KVServer {
    private static final Logger logger = Logger.getLogger(KVServer.class.getName());
    private Server server;
    private TarantoolKVClient tarantoolClient;

    public void start(int port) throws IOException {
        tarantoolClient = new TarantoolKVClient("localhost", 3301, "guest", "");
        server = ServerBuilder.forPort(port)
                .addService(new KVServiceImpl(tarantoolClient))
                .build()
                .start();

        logger.info("gRPC сервер запущен на порту " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                KVServer.this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        if (tarantoolClient != null) tarantoolClient.close();
        logger.info("Сервер остановлен");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        KVServer server = new KVServer();
        server.start(8080);
        server.server.awaitTermination();
    }
}