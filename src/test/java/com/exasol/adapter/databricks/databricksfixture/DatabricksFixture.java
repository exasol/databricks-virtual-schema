package com.exasol.adapter.databricks.databricksfixture;

import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;
import com.exasol.adapter.databricks.fixture.TestConfig;

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
            return DriverManager.getConnection(jdbcUrl);
        } catch (final SQLException exception) {
            throw new IllegalStateException(
                    "Failed connecting to JDBC URL '" + jdbcUrl + "': " + exception.getMessage(), exception);
        }
    }

    private String getJdbcUrl() {
        final URI databricksUri = config.getDatabricksHostUri();
        final String hostName = databricksUri.getHost();
        final int port = databricksUri.getPort() < 0 ? 443 : databricksUri.getPort();
        final String httpPath = "/sql/1.0/warehouses/" + getEndpoint().getId();
        return String.format("jdbc:databricks://%s:%d;transportMode=http;ssl=1;AuthMech=3;UID=token;PWD=%s;httpPath=%s",
                hostName, port, config.getDatabricksToken(), httpPath);
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
