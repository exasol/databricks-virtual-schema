package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;

import org.junit.jupiter.api.Test;

import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.ConnectionDefinition;

class JdbcLoaderIT extends AbstractIntegrationTestBase {
    @Test
    void importFromJdbcWorks() {
        final ConnectionDefinition databricksConnection = testSetup.exasol().getConnectionDefinition();
        final Table table = testSetup.databricks().createSchema().createTable("tab", "id", "INT", "name", "STRING")
                .insert(1, "a").insert(2, "b").insert(3, "c");
        final String databricksQuery = "select * from " + table.getFullyQualifiedName() + " order by id";
        final String query = "IMPORT FROM JDBC AT " + databricksConnection.getName() + " STATEMENT '" + databricksQuery
                + "'";
        testSetup.exasol().assertions().query(query, table().row(1, "a").row(2, "b").row(3, "c").matches());
    }
}
