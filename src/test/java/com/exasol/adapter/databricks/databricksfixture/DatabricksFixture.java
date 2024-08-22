package com.exasol.adapter.databricks.databricksfixture;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;
import com.exasol.adapter.databricks.TestConfig;

public class DatabricksFixture implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DatabricksFixture.class.getName());
    private final WorkspaceClient client;
    private final TestConfig config;
    private final List<DatabricksCatalog> catalogs = new ArrayList<>();

    private EndpointInfo endpoint;

    private DatabricksFixture(final WorkspaceClient client, final TestConfig config) {
        this.client = client;
        this.config = config;
    }

    public static DatabricksFixture create(final TestConfig testConfig) {
        final DatabricksConfig cfg = new DatabricksConfig() //
                .setHost(testConfig.getDatabricksHost()) //
                .setToken(testConfig.getDatabricksToken());
        final WorkspaceClient client = new WorkspaceClient(cfg);

        return new DatabricksFixture(client, testConfig);
    }

    public DatabricksCatalog createCatalog(final String name) {
        final DatabricksObjectWriter writer = new DatabricksObjectWriter(getJdbcConnection(), client, config);
        final DatabricksCatalog catalog = writer.createCatalog(name);
        this.catalogs.add(catalog);
        return catalog;
    }

    public void executeStatement(final String statement) {
        final StatementResponse response = client.statementExecution().executeStatement(
                new ExecuteStatementRequest().setWarehouseId(getWarehouseId()).setStatement(statement));
        final ServiceError error = response.getStatus().getError();
        if (error != null) {
            throw new IllegalStateException("Error executing statement: '" + statement + "': " + error.getMessage());
        }
    }

    private String getWarehouseId() {
        return getEndpoint().getId();
    }

    private Connection getJdbcConnection() {
        final String jdbcUrl = getJdbcUrl();
        try {
            return DriverManager.getConnection(jdbcUrl, getJdbcProperties());
        } catch (final SQLException exception) {
            throw new IllegalStateException("Failed connecting to JDBC URL: " + jdbcUrl, exception);
        }
    }

    private String getJdbcUrl() {
        final String hostName = config.getDatabricksHostUri().getHost();
        return String.format("jdbc:databricks://%s:443", hostName);
    }

    private Properties getJdbcProperties() {
        final Properties properties = new Properties();
        properties.put("httpPath", getHttpPath());
        properties.put("AuthMech", "3");
        properties.put("UID", "token");
        properties.put("PWD", config.getDatabricksToken());
        return properties;
    }

    private String getHttpPath() {
        return "/sql/1.0/warehouses/" + getEndpoint();
    }

    private EndpointInfo getEndpoint() {
        if (this.endpoint == null) {
            final List<EndpointInfo> endpoints = StreamSupport
                    .stream(client.warehouses().list(new ListWarehousesRequest()).spliterator(), false).toList();
            if (endpoints.size() != 1) {
                throw new IllegalStateException("Expected exactly one endpoint, but got " + endpoints.size());
            }
            this.endpoint = endpoints.get(0);
        }
        return this.endpoint;
    }

    @Override
    public void close() {
        for (final DatabricksCatalog catalog : this.catalogs) {
            catalog.drop();
        }
        this.catalogs.clear();
    }
}
