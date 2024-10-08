package com.exasol.adapter.databricks.fixture;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.Properties;
import java.util.logging.Logger;

public class TestConfig {
    private static final Logger LOGGER = Logger.getLogger(TestConfig.class.getName());
    private static final Path CONFIG_FILE = Paths.get("test.properties").toAbsolutePath();
    private final Properties properties;

    private TestConfig(final Properties properties) {
        this.properties = properties;
    }

    public static TestConfig read() {
        final Path file = CONFIG_FILE;
        if (!Files.exists(file)) {
            throw new IllegalStateException("Config file " + file + " does not exist.");
        }
        return new TestConfig(loadProperties(file));
    }

    private static Properties loadProperties(final Path configFile) {
        LOGGER.info(() -> "Reading config file " + configFile);
        try (InputStream stream = Files.newInputStream(configFile)) {
            final Properties props = new Properties();
            props.load(stream);
            return props;
        } catch (final IOException exception) {
            throw new UncheckedIOException("Error reading config file " + configFile, exception);
        }
    }

    private String getMandatoryValue(final String param) {
        if (!properties.containsKey(param)) {
            throw new IllegalStateException(
                    "Config file " + CONFIG_FILE + " does not contain parameter '" + param + "'");
        }
        return this.properties.getProperty(param);
    }

    public String getDatabricksToken() {
        return getMandatoryValue("databricks.token");
    }

    public String getDatabricksOauthSecret() {
        return getMandatoryValue("databricks.oauth.secret");
    }

    public String getDatabricksOauthClientId() {
        return getMandatoryValue("databricks.oauth.clientId");
    }

    public String getDatabricksHost() {
        return getMandatoryValue("databricks.host");
    }

    public String getDatabricksStorageRoot() {
        return getMandatoryValue("databricks.storageRoot");
    }

    public URI getDatabricksHostUri() {
        try {
            return new URI(getDatabricksHost());
        } catch (final URISyntaxException exception) {
            throw new IllegalStateException("Error parsing URI '" + getDatabricksHost() + "'", exception);
        }
    }
}
