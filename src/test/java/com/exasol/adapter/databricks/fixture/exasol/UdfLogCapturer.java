package com.exasol.adapter.databricks.fixture.exasol;

import java.io.*;
import java.net.*;
import java.util.logging.Logger;

public class UdfLogCapturer implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(UdfLogCapturer.class.getName());
    private final TcpServer server;
    private final Thread thread;

    private UdfLogCapturer(final TcpServer server, final Thread thread) {
        this.server = server;
        this.thread = thread;
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
        final TcpServer server = TcpServer.create(bindAddr);
        final Thread thread = new Thread(server, "UdfLogCapturer");
        LOG.info("Started UDF log capturer on " + server.serverSocket.getInetAddress().getHostAddress() + ":"
                + server.serverSocket.getLocalPort());
        thread.setDaemon(true);
        thread.start();
        return new UdfLogCapturer(server, thread);
    }

    @Override
    public void close() {
        this.server.running = false;
        this.thread.interrupt();
    }

    public int getPort() {
        return this.server.serverSocket.getLocalPort();
    }

    public String getServerHost() {
        return this.server.serverSocket.getInetAddress().getHostAddress();
    }

    private static class TcpServer implements Runnable {
        private boolean running = true;
        private final ServerSocket serverSocket;

        private TcpServer(final ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        private static TcpServer create(final InetAddress bindAddr) {
            return new TcpServer(createServerSocket(bindAddr));
        }

        private static ServerSocket createServerSocket(final InetAddress bindAddr) {
            try {
                return new ServerSocket(0, 50, bindAddr);
            } catch (final IOException exception) {
                throw new UncheckedIOException("Failed to create server socket", exception);
            }
        }

        @Override
        public void run() {
            while (running) {
                try (Socket clientSocket = serverSocket.accept()) {
                    readInput(clientSocket);
                } catch (final IOException exception) {
                    throw new UncheckedIOException("Failed to accept client socket", exception);
                }
            }
        }

        private void readInput(final Socket clientSocket) throws IOException {
            final BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String inputLine;
            while (running && (inputLine = in.readLine()) != null) {
                processInput(clientSocket, inputLine);
            }
        }

        private void processInput(final Socket clientSocket, final String inputLine) {
            LOG.fine(() -> "udf>" + inputLine);
        }
    }
}
