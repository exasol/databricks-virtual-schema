package com.exasol.adapter.databricks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.*;

import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture;
import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.TestConfig;
import com.exasol.dbbuilder.dialects.DatabaseObjectException;

/**
 * This class does not test the virtual schema but it verifies assumptions about the behaviour of Databricks.
 */
class DatabricksAssumptionsIT {

    private static DatabricksFixture databricks;

    @BeforeAll
    static void beforeAll() {
        final TestConfig config = TestConfig.read();
        databricks = DatabricksFixture.create(config);
    }

    @AfterAll
    static void afterAll() {
        databricks.close();
    }

    /**
     * Databricks does not support two columns with same name in different case (upper/lower).
     */
    @Test
    void columnsWithSameNameDifferentCaseNotSupported() {
        final DatabricksSchema databricksSchema = databricks.createSchema();
        final DatabaseObjectException exception = assertThrows(DatabaseObjectException.class,
                () -> databricksSchema.createTable("tab1", "col", "VARCHAR(5)", "COL", "INT"));
        assertThat(exception.getCause().getMessage(), containsString(
                "[COLUMN_ALREADY_EXISTS] The column `col` already exists. Choose another name or rename the existing column."));
    }

    /**
     * Databricks does not support two tables with same name in different case (upper/lower).
     */
    @Test
    void tablesWithSameNameDifferentCaseNotSupported() {
        final DatabricksSchema databricksSchema = databricks.createSchema();
        databricksSchema.createTable("tab", "col1", "VARCHAR(5)");
        final DatabaseObjectException exception = assertThrows(DatabaseObjectException.class,
                () -> databricksSchema.createTable("TAB", "col2", "VARCHAR(5)"));
        assertThat(exception.getCause().getMessage(), containsString(
                "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `%s`.`TAB` because it already exists."
                        .formatted(databricksSchema.getName())));
    }
}
