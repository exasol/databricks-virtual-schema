package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.Matchers.*;

import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

class PushdownQueryIT extends AbstractIntegrationTestBase {

    @TestFactory
    Stream<DynamicNode> singleTable() {
        return testSetup
                .pushdownTest(databricksSchema -> databricksSchema.createTable("tab", "ID", "INT", "NAME", "STRING")
                        .insert(1L, "a").insert(2, "b").insert(3, "c"))

                .capability("select *").query("SELECT * FROM $tab")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").row(3L, "c").matchesInAnyOrder())
                .expectPushdown(startsWith("SELECT * FROM")).done()

                .capability("SELECTLIST_PROJECTION").info("all columns uses *").query("SELECT id, name FROM $tab")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").row(3L, "c").matchesInAnyOrder())
                .expectPushdown(startsWith("SELECT * FROM")).done()

                .capability("SELECTLIST_PROJECTION").info("single column").query("SELECT id FROM $tab")
                .expect(table("BIGINT").row(1L).row(2L).row(3L).matchesInAnyOrder())
                .expectPushdown(startsWith("SELECT `tab`.`ID` FROM")).done()

                .capability("SELECTLIST_EXPRESSIONS").query("SELECT id*2, 'name: '||name FROM $tab")
                .expect(table("BIGINT", "VARCHAR").row(2L, "name: a").row(4L, "name: b").row(6L, "name: c")
                        .matchesInAnyOrder())
                .expectPushdown(startsWith("blubb")).done()

                .capability("FILTER_EXPRESSIONS").query("SELECT * FROM $tab where id = 1 or name = 'b'")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").matchesInAnyOrder())
                .expectPushdown(containsString("WHERE ((`tab`.`ID` = 1) OR (`tab`.`NAME` = 'b'))")).done()

                .capability("LIMIT").query("SELECT * FROM $tab order by id limit 2")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").matches())
                .expectPushdown(endsWith("LIMIT 2")).done()

                .capability("LIMIT_WITH_OFFSET").query("SELECT * FROM $tab order by id limit 2 offset 1")
                .expect(table("BIGINT", "VARCHAR").row(2L, "b").row(3L, "c").matches())
                .expectPushdown(endsWith("LIMIT 2 OFFSET 1")).done()

                .capability("ORDER_BY_COLUMN").query("SELECT id, name FROM $tab order by id")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").row(3L, "c").matches())
                .expectPushdown(endsWith("ORDER BY `tab`.`ID` ASC NULLS LAST")).done()

                .capability("ORDER_BY_EXPRESSION").query("SELECT id, name FROM $tab order by -id")
                .expect(table("BIGINT", "VARCHAR").row(3L, "c").row(2L, "b").row(1L, "a").matches())
                .expectPushdown(containsString("blubb")).done()

                .buildTests();
    }
}
