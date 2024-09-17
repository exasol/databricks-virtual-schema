package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;

import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

class PushdownQueryIT extends AbstractIntegrationTestBase {

    @TestFactory
    Stream<DynamicNode> singleTable() {
        return testSetup
                .pushdownTest(databricksSchema -> databricksSchema.createTable("tab", "ID", "INT", "NAME", "STRING")
                        .insert(1, "a").insert(2, "b").insert(3, "c"))
                .addTest("select *", "SELECT * FROM $tab",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matchesInAnyOrder())
                .addTest("SELECTLIST_PROJECTION - multiple columns", "SELECT id, name FROM $tab",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matchesInAnyOrder())
                .addTest("SELECTLIST_PROJECTION - single column", "SELECT id FROM $tab order by id",
                        table("DECIMAL").row(1).row(2).row(3).matches())
                .addTest("ORDER_BY_COLUMN", "SELECT id, name FROM $tab order by id",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matches())
                .addTest("SELECTLIST_EXPRESSIONS", "SELECT id*2, 'name: '||name FROM $tab order by id",
                        table("DECIMAL", "VARCHAR").row(2, "name: a").row(4, "name: b").row(6, "name: c").matches())
                .buildTests();
    }
}
