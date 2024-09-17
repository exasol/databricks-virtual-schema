package com.exasol.adapter.databricks.fixture.pushdown;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.ResultSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.*;

import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.adapter.databricks.fixture.exasol.ExasolVirtualSchema;
import com.exasol.adapter.databricks.fixture.exasol.MetadataDao.PushdownSql;
import com.exasol.dbbuilder.dialects.Table;

class PushdownTestHolder {
    private final TestSetup testSetup;
    private final ExasolVirtualSchema virtualSchema;
    private final List<Table> virtualTables;
    private final String testName;
    private final String query;
    private final Matcher<ResultSet> resultMatcher;
    private final Matcher<String> pushdownQueryMatcher;

    PushdownTestHolder(final TestSetup testSetup, final ExasolVirtualSchema virtualSchema,
            final List<Table> virtualTables, final String testName, final String query,
            final Matcher<ResultSet> resultMatcher, final Matcher<String> pushdownQueryMatcher) {
        this.testSetup = testSetup;
        this.virtualSchema = virtualSchema;
        this.virtualTables = virtualTables;
        this.testName = Objects.requireNonNull(testName, "testName");
        this.query = Objects.requireNonNull(query, "query");
        this.resultMatcher = Objects.requireNonNull(resultMatcher, "resultMatcher");
        this.pushdownQueryMatcher = Objects.requireNonNull(pushdownQueryMatcher, "pushdownQueryMatcher");
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

    private void assertResult() {
        testSetup.exasol().assertions().query(getQuery(), resultMatcher);
    }

    private void assertPushdownQuery() {
        final String vsQuery = getQuery();
        final List<PushdownSql> explainVirtual = testSetup.exasol().metadata().explainVirtual(vsQuery);
        final String reason = "Pushdown query for VS Query: " + vsQuery;
        assertAll(() -> assertThat(reason, explainVirtual, hasSize(1)),
                () -> assertThat(reason, explainVirtual.get(0).sql(), pushdownQueryMatcher));
    }

    DynamicNode toDynamicTest() {
        return DynamicContainer.dynamicContainer(testName,
                Stream.of(DynamicTest.dynamicTest("Pushdown query", this::assertPushdownQuery),
                        DynamicTest.dynamicTest("Query result", this::assertResult)));
    }
}
