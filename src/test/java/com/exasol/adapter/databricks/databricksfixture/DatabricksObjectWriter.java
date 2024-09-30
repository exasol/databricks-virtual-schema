package com.exasol.adapter.databricks.databricksfixture;

import java.sql.Connection;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import com.exasol.adapter.databricks.fixture.TestConfig;
import com.exasol.dbbuilder.dialects.*;

class DatabricksObjectWriter extends AbstractImmediateDatabaseObjectWriter {
    private final WorkspaceClient client;
    private final TestConfig config;

    protected DatabricksObjectWriter(final Connection connection, final WorkspaceClient client,
            final TestConfig config) {
        super(connection);
        this.client = client;
        this.config = config;
    }

    @Override
    public void write(final User user) {
        throw new UnsupportedOperationException("Can't create users in Databricks");
    }

    @Override
    public void write(final User user, final GlobalPrivilege... privileges) {
        throw new UnsupportedOperationException("Can't create users in Databricks");
    }

    @Override
    public void drop(final Schema schema) {
        writeToObject(schema, "DROP SCHEMA " + schema.getFullyQualifiedName() + " CASCADE");
    }

    public void drop(final DatabricksCatalog catalog) {
        writeToObject(catalog, "DROP CATALOG " + catalog.getFullyQualifiedName() + " CASCADE");
    }

    @Override
    protected String getQuotedColumnName(final String columnName) {
        return DatabricksIdentifier.of(columnName).quote();
    }

    public DatabricksCatalog createCatalog(final String name) {
        final CatalogInfo catalogInfo = client.catalogs()
                .create(new CreateCatalog().setName(name).setComment("Databricks VS integration tests")
                        .setStorageRoot(config.getDatabricksStorageRoot() + name));
        return new DatabricksCatalog(DatabricksIdentifier.of(name), catalogInfo, this);
    }

    public DatabricksSchema createSchema(final DatabricksCatalog catalog, final String name) {
        final SchemaInfo schemaInfo = client.schemas()
                .create(new CreateSchema().setName(name).setComment("Databricks VS integration tests")
                        .setStorageRoot(config.getDatabricksStorageRoot() + name).setCatalogName(catalog.getName()));
        return new DatabricksSchema(DatabricksIdentifier.of(name), schemaInfo, catalog, this);
    }
}
