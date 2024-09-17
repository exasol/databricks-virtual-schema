package com.exasol.adapter.databricks.databricksfixture;

import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import com.exasol.adapter.databricks.fixture.TestConfig;
import com.exasol.dbbuilder.dialects.*;

class DatabricksObjectWriter extends AbstractImmediateDatabaseObjectWriter {
    private static final Logger LOG = Logger.getLogger(DatabricksObjectWriter.class.getName());
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

    // Override write method because databricks does not support setting autocommit
    // See https://github.com/exasol/test-db-builder-java/issues/136
    @Override
    public void write(final Table table, final Stream<List<Object>> rows) {
        final String valuePlaceholders = "?" + ", ?".repeat(table.getColumnCount() - 1);
        final String sql = "INSERT INTO " + table.getFullyQualifiedName() + " VALUES(" + valuePlaceholders + ")";
        try (final PreparedStatement preparedStatement = this.connection.prepareStatement(sql)) {
            final AtomicInteger rowIndex = new AtomicInteger();
            rows.forEach(row -> writeRow(table, sql, preparedStatement, rowIndex.incrementAndGet(), row));
        } catch (final SQLException exception) {
            throw new DatabaseObjectException(table,
                    "Failed to create or execute prepared statement '" + sql + "' for insert.", exception);
        }
    }

    private void writeRow(final Table table, final String sql, final PreparedStatement preparedStatement,
            final int rowIndex, final List<Object> row) {
        try {
            LOG.finest("Inserting row " + rowIndex + " into table '" + table.getName() + "'...");
            for (int i = 0; i < row.size(); ++i) {
                preparedStatement.setObject(i + 1, row.get(i));
            }
            preparedStatement.execute();
        } catch (final SQLException exception) {
            if (exception.getMessage().contains(
                    "HTTP Response code: 400, Error message: BAD_REQUEST: Parameters too large: the combined size of parameters is")) {
                throw new DatabaseObjectException(table, "Size of row " + rowIndex + " with " + row.size()
                        + " columns is too large. Consider moving test values to other rows", exception);
            } else {
                throw new DatabaseObjectException(table, "Failed to execute insert query '" + sql + "' for row "
                        + rowIndex + " with " + row.size() + " columns: "+exception.getMessage(), exception);
            }
        }
    }
}
