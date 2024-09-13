package com.exasol.adapter.databricks.fixture.pushdown;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.*;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.adapter.databricks.fixture.exasol.ExasolVirtualSchema;
import com.exasol.dbbuilder.dialects.Table;

public class PushdownTestSetup {
    private final TestSetup testSetup;
    private final List<TableFactory> tableFactories;

    private PushdownTestSetup(final TestSetup testSetup, final List<TableFactory> tableFactories) {
        this.testSetup = testSetup;
        this.tableFactories = tableFactories;
    }

    public static PushdownTestSetup create(final TestSetup testSetup, final List<TableFactory> tableFactories) {
        return new PushdownTestSetup(testSetup, tableFactories);
    }

    private PushdownTestFactory createTestFactory() {
        final DatabricksSchema databricksSchema = testSetup.databricks().createSchema();
        final List<Table> databricksTables = createDatabricksTables(databricksSchema);
        final ExasolVirtualSchema virtualSchema = testSetup.exasol().createVirtualSchema(databricksSchema);
        return new PushdownTestFactory(testSetup, virtualSchema, databricksTables);
    }

    private List<Table> createDatabricksTables(final DatabricksSchema databricksSchema) {
        return tableFactories.stream().map(factory -> factory.createTable(databricksSchema)).toList();
    }

    public Stream<DynamicNode> buildTests(final String testCategory, final TestBuilder testBuilder) {
        final Stream<DynamicTest> tests = testBuilder.buildTests(createTestFactory())
                .map(PushdownTestHolder::toDynamicTest);
        return Stream.of(DynamicContainer.dynamicContainer(testCategory, tests));
    }

    @FunctionalInterface
    public static interface TestBuilder {
        Stream<PushdownTestHolder> buildTests(PushdownTestFactory factory);
    }

    @FunctionalInterface
    public static interface TableFactory {
        Table createTable(DatabricksSchema databricksSchema);
    }
}
