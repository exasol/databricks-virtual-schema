package com.exasol.adapter.databricks.databricksfixture;

import java.net.URI;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.*;
import com.exasol.adapter.databricks.fixture.CleanupActions;
import com.exasol.adapter.databricks.fixture.TestConfig;

public class DatabricksFixture implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DatabricksFixture.class.getName());
    private final WorkspaceClient client;
    private final TestConfig config;
    private final CleanupActions cleanupTasks = new CleanupActions();

    private EndpointInfo endpoint;
    private DatabricksCatalog catalog;

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

    public DatabricksSchema createSchema() {
        final DatabricksSchema schema = getCatalog().createSchema("schema-" + System.currentTimeMillis());
        LOG.fine(() -> "Created Databricks schema " + schema.getFullyQualifiedName());
        return schema;
    }

    private DatabricksCatalog getCatalog() {
        if (this.catalog == null) {
            final long timestamp = System.currentTimeMillis();
            this.catalog = createCatalog("vs-test-cat-" + timestamp);
        }
        return this.catalog;
    }

    private DatabricksCatalog createCatalog(final String name) {
        final DatabricksObjectWriter writer = new DatabricksObjectWriter(getJdbcConnection(), client, config);
        final DatabricksCatalog newCatalog = writer.createCatalog(name);
        this.cleanupTasks.add("Drop Databricks catalog " + newCatalog.getName(), newCatalog::drop);
        return newCatalog;
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
        final Properties properties = new Properties();
        properties.put("user", getJdbcUsername());
        properties.put("password", getJdbcPassword());
        LOG.fine("Connecting to '" + jdbcUrl + "'...");
        try {
            return DriverManager.getConnection(jdbcUrl, properties);
        } catch (final SQLException exception) {
            throw new IllegalStateException(
                    "Failed connecting to JDBC URL '" + jdbcUrl + "': " + exception.getMessage(), exception);
        }
    }

    public String getJdbcUrl() {
        return getJdbcUrl(AuthMode.TOKEN);
    }

    public String getJdbcUrl(final AuthMode authMode) {
        final URI databricksUri = config.getDatabricksHostUri();
        final String hostName = databricksUri.getHost();
        final int port = databricksUri.getPort() < 0 ? 443 : databricksUri.getPort();
        final String httpPath = "/sql/1.0/warehouses/" + getEndpoint().getId();
        final String oauthCredentials = authMode == AuthMode.OAUTH_M2M
                ? "OAuth2ClientId=%s;OAuth2Secret=%s;".formatted(config.getDatabricksOauthClientId(),
                        config.getDatabricksOauthSecret())
                : "";
        return "jdbc:databricks://%s:%d/default;transportMode=http;ssl=1;AuthMech=%d;Auth_Flow=%d;httpPath=%s;%s"
                .formatted(hostName, port, authMode.authMech, authMode.authFlow, httpPath, oauthCredentials);
    }

    public String getJdbcUsername() {
        return "token";
    }

    public String getJdbcPassword() {
        return config.getDatabricksToken();
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
        cleanupTasks.cleanup();
    }

    public enum AuthMode {
        TOKEN(3, -1), OAUTH_M2M(11, 1);

        private final int authMech;
        private final int authFlow;

        AuthMode(final int authMech, final int authFlow) {
            this.authMech = authMech;
            this.authFlow = authFlow;
        }
    }
}
