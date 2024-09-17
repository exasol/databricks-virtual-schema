package com.exasol.adapter.databricks.fixture.pushdown;

import java.sql.ResultSet;
import java.util.List;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.DynamicTest;

import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.adapter.databricks.fixture.exasol.ExasolVirtualSchema;
import com.exasol.dbbuilder.dialects.Table;

public class PushdownTestHolder {
    private final TestSetup testSetup;
    private final ExasolVirtualSchema virtualSchema;
    private final List<Table> virtualTables;
    private final String testName;
    private final String query;
    private final Matcher<ResultSet> expectedResultMatcher;

    PushdownTestHolder(final TestSetup testSetup, final ExasolVirtualSchema virtualSchema,
            final List<Table> virtualTables, final String testName, final String query,
            final Matcher<ResultSet> expectedResultMatcher) {
        this.testSetup = testSetup;
        this.virtualSchema = virtualSchema;
        this.virtualTables = virtualTables;
        this.testName = testName;
        this.query = query;
        this.expectedResultMatcher = expectedResultMatcher;
    }

    private String getQuery() {
        String modifiedQuery = query;
        for (final Table virtualTable : virtualTables) {
            modifiedQuery = replaceTableName(modifiedQuery, virtualTable);
        }
        return modifiedQuery;
    }

    private String replaceTableName(final String query, final Table virtualTable) {
        return replaceTableName(query, "$" + virtualTable.getName(), virtualTable);
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
        return virtualSchema.qualifyTableName(databricksTable);
    }

    private void runTest() {
        testSetup.exasol().assertions().query(getQuery(), expectedResultMatcher);
    }

    DynamicTest toDynamicTest() {
        return DynamicTest.dynamicTest(testName, this::runTest);
    }
}
