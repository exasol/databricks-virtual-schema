package com.exasol.adapter.databricks;

import java.io.*;
import java.nio.file.*;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

import com.exasol.errorreporting.ExaError;

class TestConfig {
    private static final Logger LOGGER = Logger.getLogger(TestConfig.class.getName());
    private static final Path CONFIG_FILE = Paths.get("test.properties").toAbsolutePath();
    private final Properties properties;

    private TestConfig(final Properties properties) {
        this.properties = properties;
    }

    static TestConfig read() {
        final Path file = CONFIG_FILE;
        if (!Files.exists(file)) {
            throw new IllegalStateException("Config file " + file + " does not exist.");
        }
        return new TestConfig(loadProperties(file));
    }

    static Properties loadProperties(final Path configFile) {
        LOGGER.info(() -> "Reading config file " + configFile);
        try (InputStream stream = Files.newInputStream(configFile)) {
            final Properties props = new Properties();
            props.load(stream);
            return props;
        } catch (final IOException e) {
            throw new UncheckedIOException(ExaError.messageBuilder("E-EITFJ-26")
                    .message("Error reading config file {{config file path}}", configFile).toString(), e);
        }
    }

    private Optional<String> getOptionalValue(final String param) {
        return Optional.ofNullable(this.properties.getProperty(param));
    }

    private String getMandatoryValue(final String param) {
        if (!properties.containsKey(param)) {
            throw new IllegalStateException(
                    "Config file " + CONFIG_FILE + " does not contain parameter '" + param + "'");
        }
        return this.properties.getProperty(param);
    }

    String getDatabricksToken() {
        return getMandatoryValue("databricks.token");
    }

    String getDatabricksHost() {
        return getMandatoryValue("databricks.host");
    }
}
