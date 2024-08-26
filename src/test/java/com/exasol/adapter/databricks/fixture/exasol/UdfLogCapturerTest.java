package com.exasol.adapter.databricks.fixture.exasol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

class UdfLogCapturerTest {

    @Test
    void closesInShortTime() {
        final UdfLogCapturer udfLogCapturer = UdfLogCapturer.start();
        final Instant start = Instant.now();
        udfLogCapturer.close();
        final Duration stopDuration = Duration.between(start, Instant.now());
        assertTrue(stopDuration.minus(Duration.ofMillis(700)).isNegative(),
                "Closing the UdfLogCapturer took longer than 1 second: " + stopDuration);
    }

    @Test
    void acceptsTcpConnections() throws IOException {
        try (final UdfLogCapturer udfLogCapturer = UdfLogCapturer.start()) {
            try (final Socket socket = new Socket(udfLogCapturer.getServerHost(), udfLogCapturer.getPort());
                    OutputStreamWriter output = new OutputStreamWriter(socket.getOutputStream())) {
                assertDoesNotThrow(() -> {
                    output.write("Hello, World!\n");
                    output.flush();
                });
            }
        }
    }
}
