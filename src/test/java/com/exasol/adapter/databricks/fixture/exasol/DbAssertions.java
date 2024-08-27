package com.exasol.adapter.databricks.fixture.exasol;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.*;

import org.hamcrest.Matcher;

import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

public class DbAssertions {

    private final Connection connection;

    DbAssertions(final Connection connection) {
        this.connection = connection;
    }

    public void query(final String query, final Matcher<ResultSet> matcher) {
        try (final Statement statement = this.connection.createStatement();
                final ResultSet resultSet = statement.executeQuery(query)) {
            assertThat(resultSet, matcher);
        } catch (final SQLException exception) {
            throw new IllegalStateException("Unable to execute query: '" + query + "'", exception);
        }
    }

    public void virtualSchemaExists(final VirtualSchema virtualSchema) {
        query("""
                select SCHEMA_NAME, ADAPTER_SCRIPT_SCHEMA, ADAPTER_SCRIPT_NAME, ADAPTER_NOTES
                from EXA_ALL_VIRTUAL_SCHEMAS
                """, table().row(virtualSchema.getName(), "ADAPTER_SCRIPT_SCHEMA", "DATABRICKS_VS_ADAPTER", "notes")
                .matches());
    }
}
