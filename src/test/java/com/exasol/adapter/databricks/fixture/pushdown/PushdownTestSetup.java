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
import com.exasol.matcher.ResultSetStructureMatcher;

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

    public PushdownTestBuilder capability(final String capability) {
        // this.tests.add(this.testFactory.create(testName, query, expectedResultMatcher));
        return new PushdownTestBuilder(this, capability);
    }

    public Stream<DynamicNode> buildTests() {
        return this.tests.stream().map(PushdownTestHolder::toDynamicTest);
    }

    public static class PushdownTestBuilder {
        private final String capability;
        private final PushdownTestSetup testSetup;
        private String testInfo;
        private String query;
        private Matcher<ResultSet> expectedResultMatcher;

        private PushdownTestBuilder(final PushdownTestSetup testSetup, final String capability) {
            this.testSetup = testSetup;
            this.capability = capability;
        }

        public PushdownTestBuilder info(final String testInfo) {
            this.testInfo = testInfo;
            return this;
        }

        public PushdownTestBuilder query(final String query) {
            this.query = query;
            return this;
        }

        public PushdownTestBuilder expect(final ResultSetStructureMatcher.Builder expectedResultMatcher) {
            return this.expect(expectedResultMatcher.matches());
        }

        public PushdownTestBuilder expect(final Matcher<ResultSet> expectedResultMatcher) {
            this.expectedResultMatcher = expectedResultMatcher;
            return this;
        }

        public PushdownTestSetup done() {
            final String testName = capability + (testInfo != null ? " " + testInfo : "");
            testSetup.tests.add(testSetup.testFactory.create(testName, query, expectedResultMatcher));
            return testSetup;
        }
    }

    @FunctionalInterface
    public static interface TableFactory {
        Table createTable(DatabricksSchema databricksSchema);
    }
}
