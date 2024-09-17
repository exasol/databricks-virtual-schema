package com.exasol.adapter.databricks.fixture.pushdown;

import java.sql.ResultSet;
import java.util.List;

import org.hamcrest.Matcher;

import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.adapter.databricks.fixture.exasol.ExasolVirtualSchema;
import com.exasol.dbbuilder.dialects.Table;

class PushdownTestFactory {
    private final TestSetup testSetup;
    private final ExasolVirtualSchema virtualSchema;
    private final List<Table> virtualTables;

    PushdownTestFactory(final TestSetup testSetup, final ExasolVirtualSchema virtualSchema,
            final List<Table> virtualTables) {
        this.testSetup = testSetup;
        this.virtualSchema = virtualSchema;
        this.virtualTables = virtualTables;
    }

    PushdownTestHolder create(final String testName, final String query,
            final Matcher<ResultSet> expectedResultMatcher) {
        return new PushdownTestHolder(testSetup, virtualSchema, virtualTables, testName, query, expectedResultMatcher);
    }
}
