package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import com.exasol.adapter.databricks.fixture.pushdown.*;

class PushdownQueryIT extends AbstractIntegrationTestBase {
    Stream<PushdownTestHolder> pushdownQueryTestData(final PushdownTestFactory factory) {
        return Stream.of(
                factory.create("select *", "SELECT * FROM $VIRTUAL_TABLE",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matchesInAnyOrder()),
                factory.create("SELECTLIST_PROJECTION", "SELECT id, name FROM $VIRTUAL_TABLE",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matchesInAnyOrder()),
                factory.create("SELECTLIST_PROJECTION - single column", "SELECT id FROM $VIRTUAL_TABLE order by id",
                        table("DECIMAL").row(1).row(2).row(3).matches()),
                factory.create("ORDER_BY_COLUMN", "SELECT id, name FROM $VIRTUAL_TABLE order by id",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matches()),
                factory.create("SELECTLIST_EXPRESSIONS", "SELECT id*2, 'name: '||name FROM $VIRTUAL_TABLE order by id",
                        table("DECIMAL", "VARCHAR").row(2, "name: a").row(4, "name: b").row(6, "name: c").matches()));
    }

    @TestFactory
    Stream<DynamicNode> pushdown() {
        final PushdownTestSetup pushdownSetup = PushdownTestSetup.create(testSetup,
                List.of(databricksSchema -> databricksSchema.createTable("tab", "ID", "INT", "NAME", "STRING")
                        .insert(1, "a").insert(2, "b").insert(3, "c")));
        return pushdownSetup.buildTests("single table", this::pushdownQueryTestData);
    }
}
