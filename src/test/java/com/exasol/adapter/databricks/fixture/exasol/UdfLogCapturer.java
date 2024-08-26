package com.exasol.adapter.databricks.fixture.exasol;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class UdfLogCapturer implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(UdfLogCapturer.class.getName());
    private static final Duration SOCKET_TIMEOUT = Duration.ofMillis(500);

    private final TcpServer server;
    private final ExecutorService executorService;
    private final Future<?> serverFuture;

    private UdfLogCapturer(final TcpServer server, final Future<?> serverFuture,
            final ExecutorService executorService) {
        this.server = server;
        this.serverFuture = serverFuture;
        this.executorService = executorService;
    }

    public static UdfLogCapturer start() {
        return start(getLocalAddress());
    }

    private static InetAddress getLocalAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (final UnknownHostException exception) {
            throw new IllegalStateException("Failed to get local host address", exception);
        }
    }

    public static UdfLogCapturer start(final InetAddress bindAddr) {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final TcpServer server = TcpServer.create(bindAddr);
        final Future<?> serverFuture = executorService.submit(server);
        LOG.info("Started UDF log capturer on " + server.serverSocket.getInetAddress().getHostAddress() + ":"
                + server.serverSocket.getLocalPort());
        return new UdfLogCapturer(server, serverFuture, executorService);
    }

    @Override
    public void close() {
        this.server.running.set(false);
        waitUntilServerStopped(SOCKET_TIMEOUT.plusMillis(100));
    }

    private void waitUntilServerStopped(final Duration timeout) {
        LOG.fine(() -> "Waiting " + timeout + " until TCP server is stopped...");
        final Instant start = Instant.now();
        try {
            serverFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (final ExecutionException exception) {
            throw new IllegalStateException("Failed to stop server: " + exception.getMessage(), exception);
        } catch (final TimeoutException exception) {
            throw new IllegalStateException("Server did not stop within " + timeout + ": " + exception.getMessage(),
                    exception);
        }
        LOG.fine("Server stopped after " + Duration.between(start, Instant.now()));
    }

    public int getPort() {
        return this.server.serverSocket.getLocalPort();
    }

    public String getServerHost() {
        return this.server.serverSocket.getInetAddress().getHostAddress();
    }

    private static class TcpServer implements Runnable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final ServerSocket serverSocket;

        private TcpServer(final ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        private static TcpServer create(final InetAddress bindAddr) {
            return new TcpServer(createServerSocket(bindAddr));
        }

        private static ServerSocket createServerSocket(final InetAddress bindAddr) {
            try {
                final ServerSocket socket = new ServerSocket(0, 50, bindAddr);
                socket.setSoTimeout((int) SOCKET_TIMEOUT.toMillis());
                return socket;
            } catch (final IOException exception) {
                throw new UncheckedIOException("Failed to create server socket", exception);
            }
        }

        @Override
        public void run() {
            while (running.get()) {
                try (Socket clientSocket = serverSocket.accept()) {
                    final ClientListener clientListener = new ClientListener(clientSocket, running);
                    clientListener.run();
                } catch (final SocketTimeoutException exception) {
                    // Ignore, continue to accept new connections
                } catch (final IOException exception) {
                    throw new UncheckedIOException("Failed to accept client socket", exception);
                }
            }
            LOG.fine("TCP server loop finished");
        }
    }

    private static class ClientListener implements Runnable {
        private static final AtomicInteger clientCounter = new AtomicInteger(0);
        private final int clientId;
        private final Socket clientSocket;
        private final AtomicBoolean running;

        public ClientListener(final Socket clientSocket, final AtomicBoolean running) {
            this.clientSocket = clientSocket;
            this.running = running;
            this.clientId = clientCounter.incrementAndGet();
        }

        @Override
        public void run() {
            LOG.fine(() -> "Client#" + clientId + ": connected from " + clientSocket.getRemoteSocketAddress());
            processClientInput();
            LOG.fine(() -> "Client#" + clientId + ": disconnected");
        }

        private void processClientInput() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                String inputLine;
                while (running.get() && (inputLine = in.readLine()) != null) {
                    processInput(inputLine);
                }
            } catch (final SocketException exception) {
                if (exception.getMessage().equals("Socket is closed")) {
                    LOG.fine("Client#" + clientId + ": Socket " + clientSocket + " closed: " + exception);
                    return;
                }
                throw new UncheckedIOException("Failed to read from client #" + clientId + " at socket "
                        + this.clientSocket + ": " + exception, exception);
            } catch (final IOException exception) {
                throw new UncheckedIOException("Failed to read from client #" + clientId + " at socket "
                        + this.clientSocket + ": " + exception, exception);
            }
        }

        private void processInput(final String inputLine) {
            LOG.fine(() -> "Client#" + clientId + ">" + inputLine);
        }
    }
}
