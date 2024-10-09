package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture.AuthMode;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.ConnectionDefinition;

class JdbcLoaderIT extends AbstractIntegrationTestBase {

    @ParameterizedTest
    @EnumSource(AuthMode.class)
    void importFromJdbcWorks(final AuthMode authMode) {
        final Table table = testSetup.databricks().createSchema().createTable("TAB", "ID", "INT", "NAME", "STRING")
                .bulkInsert(Stream.of(List.of(1, "a"), List.of(2, "b"), List.of(3, "c")));
        final String databricksQuery = "SELECT * FROM " + table.getFullyQualifiedName() + " ORDER BY ID";
        final String query = "IMPORT FROM JDBC AT " + getConnectionName(authMode) + " STATEMENT '" + databricksQuery
                + "'";
        testSetup.exasol().assertions().query(query, table().row(1L, "a").row(2L, "b").row(3L, "c").matches());
    }

    private String getConnectionName(final AuthMode authMode) {
        final ConnectionDefinition databricksConnection = testSetup.exasol().getConnectionDefinition(authMode);
        return databricksConnection.getName();
    }
}
