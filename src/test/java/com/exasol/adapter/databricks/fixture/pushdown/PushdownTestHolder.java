package com.exasol.adapter.databricks.fixture.pushdown;

import java.sql.ResultSet;
import java.util.List;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.DynamicTest;

import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

public class PushdownTestHolder {
    private final TestSetup testSetup;
    private final VirtualSchema virtualSchema;
    private final List<Table> virtualTables;
    private final String testName;
    private final String query;
    private final Matcher<ResultSet> expectedResultMatcher;

    PushdownTestHolder(final TestSetup testSetup, final VirtualSchema virtualSchema, final List<Table> virtualTables,
            final String testName, final String query, final Matcher<ResultSet> expectedResultMatcher) {
        this.testSetup = testSetup;
        this.virtualSchema = virtualSchema;
        this.virtualTables = virtualTables;
        this.testName = testName;
        this.query = query;
        this.expectedResultMatcher = expectedResultMatcher;
    }

    private String getQuery() {
        if (this.virtualTables.size() == 1) {
            return replaceTableName(query, "$VIRTUAL_TABLE", virtualTables.get(0));
        }
        return replaceAllTableNames();
    }

    private String replaceAllTableNames() {
        String modifiedQuery = query;
        for (int i = 0; i < virtualTables.size(); i++) {
            modifiedQuery = replaceTableName(modifiedQuery, i);
        }
        return modifiedQuery;
    }

    private String replaceTableName(final String query, final int tableIndex) {
        return replaceTableName(query, "$VIRTUAL_TABLE" + tableIndex, virtualTables.get(tableIndex));
    }

    private String replaceTableName(final String query, final String placeholder, final Table table) {
        final String modifiedQuery = query.replace(placeholder, virtualTableName(table));
        if (query.equals(modifiedQuery)) {
            throw new IllegalArgumentException(
                    "Query '" + query + "' does not contain placeholder '" + placeholder + "'");
        }
        return modifiedQuery;
    }

    private String virtualTableName(final Table databricksTable) {
        return String.format("\"%s\".\"%s\"", virtualSchema.getName(), databricksTable.getName());
    }

    private void runTest() {
        testSetup.exasol().assertions().query(getQuery(), expectedResultMatcher);
    }

    DynamicTest toDynamicTest() {
        return DynamicTest.dynamicTest(testName, this::runTest);
    }
}
