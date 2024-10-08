package com.exasol.adapter.databricks.databricksfixture;

import java.net.URI;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
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
        final DatabricksSchema schema = getCatalog().createSchema("schema_" + System.currentTimeMillis());
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
        final Connection jdbcConnection = getJdbcConnection(AuthMode.TOKEN);
        final DatabricksObjectWriter writer = new DatabricksObjectWriter(jdbcConnection, client, config);
        final DatabricksCatalog newCatalog = writer.createCatalog(name);
        LOG.fine(() -> "Created Databricks catalog " + newCatalog.getFullyQualifiedName());
        this.cleanupTasks.add("Drop Databricks catalog " + newCatalog.getName(), newCatalog::drop);
        grantAccessToServicePrincipal(newCatalog);
        return newCatalog;
    }

    private void grantAccessToServicePrincipal(final DatabricksCatalog newCatalog) {
        executeStatement("grant use catalog, use schema, select on catalog " + newCatalog.getFullyQualifiedName()
                + " to `" + config.getDatabricksOauthServicePrincipalUuid() + "`");
    }

    public void executeStatement(final String statement) {
        final Instant start = Instant.now();
        final StatementResponse response = client.statementExecution().executeStatement(
                new ExecuteStatementRequest().setWarehouseId(getWarehouseId()).setStatement(statement));
        LOG.fine("Executed Databricks statement '" + statement + "' in " + Duration.between(start, Instant.now()));
        final ServiceError error = response.getStatus().getError();
        if (error != null) {
            throw new IllegalStateException("Error executing statement: '" + statement + "': " + error.getMessage());
        }
    }

    private String getWarehouseId() {
        return getEndpoint().getId();
    }

    private Connection getJdbcConnection(final AuthMode authMode) {
        final String jdbcUrl = getJdbcUrl(authMode);
        final Properties properties = new Properties();
        properties.put("user", getJdbcUsername(authMode));
        properties.put("password", getJdbcPassword(authMode));
        LOG.fine("Connecting to '" + jdbcUrl + "'...");
        try {
            return DriverManager.getConnection(jdbcUrl, properties);
        } catch (final SQLException exception) {
            throw new IllegalStateException(
                    "Failed connecting to JDBC URL '" + jdbcUrl + "': " + exception.getMessage(), exception);
        }
    }

    public String getJdbcUrl(final AuthMode authMode) {
        final URI databricksUri = config.getDatabricksHostUri();
        final String hostName = databricksUri.getHost();
        final int port = databricksUri.getPort() < 0 ? 443 : databricksUri.getPort();
        final String httpPath = "/sql/1.0/warehouses/" + getEndpoint().getId();
        final String oauthCredentials = (authMode == AuthMode.OAUTH_M2M)
                ? "OAuth2ClientId=%s;OAuth2Secret=%s;".formatted(config.getDatabricksOauthClientId(),
                        config.getDatabricksOauthSecret())
                : "";
        return "jdbc:databricks://%s:%d/default;transportMode=http;ssl=1;AuthMech=%d;Auth_Flow=%d;httpPath=%s;%s"
                .formatted(hostName, port, authMode.authMech, authMode.authFlow, httpPath, oauthCredentials);
    }

    public String getJdbcUsername(final AuthMode authMode) {
        return authMode == AuthMode.TOKEN ? "token" : null;
    }

    public String getJdbcPassword(final AuthMode authMode) {
        return authMode == AuthMode.TOKEN ? config.getDatabricksToken() : null;
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
