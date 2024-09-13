package com.exasol.adapter.databricks.fixture.exasol;

import static java.util.stream.Collectors.joining;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Stream;

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
        final TcpServer server = TcpServer.create(bindAddr, executorService);
        final Future<?> serverFuture = executorService.submit(server);
        LOG.info("Started UDF log capturer on " + server.serverSocket.getInetAddress().getHostAddress() + ":"
                + server.serverSocket.getLocalPort());
        return new UdfLogCapturer(server, serverFuture, executorService);
    }

    public int getPort() {
        return this.server.serverSocket.getLocalPort();
    }

    public String getServerHost() {
        return this.server.serverSocket.getInetAddress().getHostAddress();
    }

    public List<String> getCollectedLines() {
        return this.server.getCollectedLines();
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
        LOG.fine(() -> "Udf log capturer stopped after " + Duration.between(start, Instant.now()));
    }

    private static class TcpServer implements Runnable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final ServerSocket serverSocket;
        private final Executor executor;
        private final List<ClientListener> clientListeners = new CopyOnWriteArrayList<>();

        private TcpServer(final ServerSocket serverSocket, final Executor executor) {
            this.serverSocket = serverSocket;
            this.executor = executor;
        }

        private static TcpServer create(final InetAddress bindAddr, final ExecutorService executorService) {
            return new TcpServer(createServerSocket(SOCKET_TIMEOUT, bindAddr), executorService);
        }

        private static ServerSocket createServerSocket(final Duration socketTimeout, final InetAddress bindAddr) {
            try {
                final ServerSocket socket = new ServerSocket(0, 50, bindAddr);
                socket.setSoTimeout((int) socketTimeout.toMillis());
                return socket;
            } catch (final IOException exception) {
                throw new UncheckedIOException("Failed to create server socket", exception);
            }
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    final Socket clientSocket = serverSocket.accept();
                    final ClientListener clientListener = new ClientListener(clientSocket, running);
                    clientListeners.add(clientListener);
                    executor.execute(clientListener);
                } catch (final SocketTimeoutException exception) {
                    // ignore
                } catch (final IOException exception) {
                    throw new UncheckedIOException("Failed to accept client socket", exception);
                }
            }
            LOG.fine("TCP server loop finished");
        }

        private List<String> getCollectedLines() {
            return clientListeners.stream().flatMap(ClientListener::getCollectedLines).toList();
        }
    }

    private static class ClientListener implements Runnable {
        private static final AtomicInteger clientCounter = new AtomicInteger(0);
        private final int clientId;
        private final Socket clientSocket;
        private final AtomicBoolean running;
        private final List<String> collectedLines = new CopyOnWriteArrayList<>();

        public ClientListener(final Socket clientSocket, final AtomicBoolean running) {
            this.clientSocket = clientSocket;
            this.running = running;
            this.clientId = clientCounter.incrementAndGet();
        }

        @Override
        public void run() {
            LOG.fine(() -> "Client#" + clientId + ": connected from " + clientSocket.getRemoteSocketAddress());
            processClientInput();
            LOG.fine(() -> "Client#" + clientId + ": disconnected after logging " + collectedLines.size() + " lines:\n"
                    + getCollectedLines().collect(joining("\nClient#" + clientId + ">")));
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
            } finally {
                try {
                    clientSocket.close();
                } catch (final IOException exception) {
                    throw new UncheckedIOException("Failed to close client socket", exception);
                }
            }
        }

        private void processInput(final String inputLine) {
            collectedLines.add(inputLine);
        }

        private Stream<String> getCollectedLines() {
            return collectedLines.stream();
        }
    }
}
