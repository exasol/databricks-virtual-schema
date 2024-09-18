package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;

import org.junit.jupiter.api.Test;

import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.ConnectionDefinition;

class JdbcLoaderIT extends AbstractIntegrationTestBase {
    @Test
    void importFromJdbcWorks() {
        final Table table = testSetup.databricks().createSchema().createTable("TAB", "ID", "INT", "NAME", "STRING")
                .insert(1, "a").insert(2, "b").insert(3, "c");
        final String databricksQuery = "SELECT * FROM " + table.getFullyQualifiedName() + " ORDER BY ID";
        final String query = "IMPORT FROM JDBC AT " + getConnectionName() + " STATEMENT '" + databricksQuery + "'";
        testSetup.exasol().assertions().query(query, table().row(1L, "a").row(2L, "b").row(3L, "c").matches());
    }

    private String getConnectionName() {
        final ConnectionDefinition databricksConnection = testSetup.exasol().getConnectionDefinition();
        return databricksConnection.getName();
    }
}
