package com.exasol.adapter.databricks.fixture.pushdown;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.DynamicNode;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.adapter.databricks.fixture.exasol.ExasolVirtualSchema;
import com.exasol.dbbuilder.dialects.Table;

public class PushdownTestSetup {
    private final PushdownTestFactory testFactory;
    private final List<PushdownTestHolder> tests = new ArrayList<>();

    private PushdownTestSetup(final PushdownTestFactory testFactory) {
        this.testFactory = testFactory;
    }

    public static PushdownTestSetup create(final TestSetup testSetup, final List<TableFactory> tableFactories) {
        final PushdownTestFactory testFactory = createTestFactory(testSetup, tableFactories);
        return new PushdownTestSetup(testFactory);
    }

    private static PushdownTestFactory createTestFactory(final TestSetup testSetup,
            final List<TableFactory> tableFactories) {
        final DatabricksSchema databricksSchema = testSetup.databricks().createSchema();
        final List<Table> databricksTables = createDatabricksTables(databricksSchema, tableFactories);
        final ExasolVirtualSchema virtualSchema = testSetup.exasol().createVirtualSchema(databricksSchema);
        return new PushdownTestFactory(testSetup, virtualSchema, databricksTables);
    }

    private static List<Table> createDatabricksTables(final DatabricksSchema databricksSchema,
            final List<TableFactory> tableFactories) {
        return tableFactories.stream().map(factory -> factory.createTable(databricksSchema)).toList();
    }

    public PushdownTestSetup addTest(final String testName, final String query,
            final Matcher<ResultSet> expectedResultMatcher) {
        this.tests.add(this.testFactory.create(testName, query, expectedResultMatcher));
        return this;
    }

    public Stream<DynamicNode> buildTests() {
        return this.tests.stream().map(PushdownTestHolder::toDynamicTest);
    }

    @FunctionalInterface
    public static interface TableFactory {
        Table createTable(DatabricksSchema databricksSchema);
    }
}
