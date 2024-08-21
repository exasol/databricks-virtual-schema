package com.exasol.adapter.databricks;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CreateCatalog;
import com.databricks.sdk.service.workspace.ObjectInfo;

class DatabricksFixture implements AutoCloseable {

    private final WorkspaceClient client;
    private final TestConfig config;

    private DatabricksFixture(final WorkspaceClient client, final TestConfig config) {
        this.client = client;
        this.config = config;
    }

    static DatabricksFixture create(final TestConfig testConfig) {
        final DatabricksConfig cfg = new DatabricksConfig() //
                .setHost(testConfig.getDatabricksHost()) //
                .setToken(testConfig.getDatabricksToken());
        final WorkspaceClient client = new WorkspaceClient(cfg);

        return new DatabricksFixture(client, testConfig);
    }

    void createTable() {
        final long timestamp = System.currentTimeMillis();
        System.out.println("workspace list");
        for (final ObjectInfo o : client.workspace().list("/Workspace/Users/christoph.pirkl@exasol.com/data/")) {
            System.out.println(o.getPath());
        }

        // client.metastores().create(
        // new CreateMetastore().setName("databrics-vs-metastore-" + timestamp).setStorageRoot("storage root"));
        final CatalogInfo catalog = client.catalogs()
                .create(new CreateCatalog().setName("databrics-vs-catalog-" + timestamp)
                        .setComment("Databricks VS integration tests")
                        .setStorageRoot("/Workspace/Users/christoph.pirkl@exasol.com/data/"));
        createTable(catalog.getName(), "mySchema", "myTable");
    }

    void createTable(final String catalogName, final String schemaName, final String tableName) {
        final String createTableJson = String.format(
                "{\"name\": \"%s\", \"catalog_name\": \"%s\", \"schema_name\": \"%s\"}", tableName, catalogName,
                schemaName);

        final HttpRequest request = HttpRequest.newBuilder().uri(uri("/api/2.0/unity-catalog/tables"))
                .header("Authorization", "Bearer " + config.getDatabricksToken())
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(createTableJson))
                .build();

        final HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException exception) {
            throw new IllegalStateException("Failed to execute request " + request, exception);
        }

        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to create table: " + response.body());
        }
    }

    private URI uri(final String path) {
        final String uri = config.getDatabricksHost() + path;
        try {
            return new URI(uri);
        } catch (final URISyntaxException exception) {
            throw new IllegalStateException("Failed to create URI '" + uri + "'", exception);
        }
    }

    @Override
    public void close() {
        // Nothing to do here
    }
}
