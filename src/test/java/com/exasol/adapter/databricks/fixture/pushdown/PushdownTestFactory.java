package com.exasol.adapter.databricks.fixture.pushdown;

import java.sql.ResultSet;
import java.util.List;

import org.hamcrest.Matcher;

import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

public class PushdownTestFactory {
    private final TestSetup testSetup;
    private final VirtualSchema virtualSchema;
    private final List<Table> virtualTables;

    public PushdownTestFactory(final TestSetup testSetup, final VirtualSchema virtualSchema,
            final List<Table> virtualTables) {
        this.testSetup = testSetup;
        this.virtualSchema = virtualSchema;
        this.virtualTables = virtualTables;
    }

    public PushdownTestHolder create(final String testName, final String query,
            final Matcher<ResultSet> expectedResultMatcher) {
        return new PushdownTestHolder(testSetup, virtualSchema, virtualTables, testName, query, expectedResultMatcher);
    }
}
