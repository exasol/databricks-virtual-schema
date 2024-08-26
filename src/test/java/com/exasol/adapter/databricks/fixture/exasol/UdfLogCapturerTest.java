package com.exasol.adapter.databricks.fixture.exasol;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

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
    void acceptsNoCollectedLinesWithoutClient() {
        try (final UdfLogCapturer udfLogCapturer = UdfLogCapturer.start()) {
            assertThat(udfLogCapturer.getCollectedLines(), empty());
        }
    }

    @Test
    void acceptsTcpConnections() throws IOException, InterruptedException {
        try (final UdfLogCapturer udfLogCapturer = UdfLogCapturer.start()) {
            sendLines(udfLogCapturer, List.of("Hello, World!"));
            Thread.sleep(10); // wait for the server to process the lines
            assertThat(udfLogCapturer.getCollectedLines(), contains("Hello, World!"));
        }
    }

    private void sendLines(final UdfLogCapturer udfLogCapturer, final List<String> lines) throws IOException {
        try (final Socket socket = new Socket(udfLogCapturer.getServerHost(), udfLogCapturer.getPort());
                OutputStreamWriter output = new OutputStreamWriter(socket.getOutputStream())) {
            for (final String line : lines) {
                output.write(line + "\n");
            }
        }
    }
}
