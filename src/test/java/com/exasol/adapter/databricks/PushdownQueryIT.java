package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.Matchers.*;

import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import com.exasol.matcher.TypeMatchMode;

class PushdownQueryIT extends AbstractIntegrationTestBase {

    private static final String BROKEN_PUSHDOWN = "Pushdown not yet working, need to investigate root cause";
    private static final String TODO = "TODO!";

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
                .expectPushdown(startsWith(BROKEN_PUSHDOWN)).done()

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
                .expectPushdown(containsString(BROKEN_PUSHDOWN)).done()

                .buildTests();
    }

    @TestFactory
    Stream<DynamicNode> join() {
        return testSetup
                .pushdownTest(
                        databricksSchema -> databricksSchema
                                .createTable("customers", "CID", "INT", "NAME", "STRING", "COUNTRY", "STRING")
                                .insert(1, "c1", "a").insert(2, "c2", "a").insert(3, "c3", "b"),
                        databricksSchema -> databricksSchema
                                .createTable("orders", "OID", "INT", "CUST_ID", "INT", "DATE", "DATE")
                                .insert(10, 1, "2024-09-01").insert(11, 2, "2024-09-02").insert(12, 2, "2024-09-03"))

                .capability("JOIN_CONDITION_ALL")
                .query("select NAME, COUNTRY, OID from $customers, $orders order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c1", "a", 11).row("c1", "a", 12) //
                        .row("c2", "a", 10).row("c2", "a", 11).row("c2", "a", 12) //
                        .row("c3", "b", 10).row("c3", "b", 11).row("c3", "b", 12) //
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(containsString(TODO)).done()

                .capability("JOIN_CONDITION_EQUI")
                .query("select NAME, COUNTRY, OID from $customers join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT").row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("INNER JOIN"),containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)"))).done()

                .capability("JOIN_TYPE_INNER")
                .query("select NAME, COUNTRY, OID from $customers inner join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT").row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("INNER JOIN"), containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)"))).done()

                .capability("JOIN_TYPE_FULL_OUTER")
                .query("select NAME, COUNTRY, OID from $customers full outer join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12).row("c3", "b", null)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("FULL OUTER JOIN"), containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)"))).done()

                .capability("JOIN_TYPE_LEFT_OUTER")
                .query("select NAME, COUNTRY, OID from $customers left outer join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12).row("c3", "b", null)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("LEFT OUTER JOIN"), containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)"))).done()

                .capability("JOIN_TYPE_RIGHT_OUTER")
                .query("select NAME, COUNTRY, OID from $customers right outer join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("RIGHT OUTER JOIN"), containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)"))).done()

                .buildTests();
    }
}
