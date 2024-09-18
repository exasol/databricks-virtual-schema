package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.Matchers.*;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import com.exasol.adapter.databricks.fixture.pushdown.PushdownTestSetup.TableFactory;
import com.exasol.matcher.TypeMatchMode;

class PushdownQueryIT extends AbstractIntegrationTestBase {

    @TestFactory
    Stream<DynamicNode> selectlistFilterOrderLimit() {
        return testSetup
                .pushdownTest(databricksSchema -> databricksSchema.createTable("tab", "ID", "INT", "NAME", "STRING")
                        .bulkInsert(Stream.of(List.of(1L, "a"), List.of(2, "b"), List.of(3, "c"))))

                .capability("select *").query("SELECT * FROM $tab")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").row(3L, "c").matchesInAnyOrder())
                .expectPushdown(startsWith("SELECT * FROM"))

                .capability("SELECTLIST_PROJECTION").info("all columns uses *").query("SELECT id, name FROM $tab")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").row(3L, "c").matchesInAnyOrder())
                .expectPushdown(startsWith("SELECT * FROM"))

                .capability("SELECTLIST_PROJECTION").info("single column").query("SELECT id FROM $tab")
                .expect(table("BIGINT").row(1L).row(2L).row(3L).matchesInAnyOrder())
                .expectPushdown(startsWith("SELECT `tab`.`ID` FROM"))

                .capability("SELECTLIST_EXPRESSIONS").query("SELECT id*2, name FROM $tab")
                .expect(table("BIGINT", "VARCHAR").row(2L, "a").row(4L, "b").row(6L, "c").matchesInAnyOrder())
                .expectPushdown(startsWith("SELECT xy2"))

                .capability("FILTER_EXPRESSIONS").query("SELECT * FROM $tab where id = 1 or name = 'b'")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").matchesInAnyOrder())
                .expectPushdown(containsString("WHERE ((`tab`.`ID` = 1) OR (`tab`.`NAME` = 'b'))"))

                .capability("LIMIT").query("SELECT * FROM $tab order by id limit 2")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").matches())
                .expectPushdown(endsWith("LIMIT 2"))

                .capability("LIMIT_WITH_OFFSET").query("SELECT * FROM $tab order by id limit 2 offset 1")
                .expect(table("BIGINT", "VARCHAR").row(2L, "b").row(3L, "c").matches())
                .expectPushdown(endsWith("LIMIT 2 OFFSET 1"))

                .capability("ORDER_BY_COLUMN").query("SELECT id, name FROM $tab order by id")
                .expect(table("BIGINT", "VARCHAR").row(1L, "a").row(2L, "b").row(3L, "c").matches())
                .expectPushdown(endsWith("ORDER BY `tab`.`ID` ASC NULLS LAST"))

                .capability("ORDER_BY_EXPRESSION").query("SELECT id, name FROM $tab order by -id")
                .expect(table("BIGINT", "VARCHAR").row(3L, "c").row(2L, "b").row(1L, "a").matches())
                .pushdownNotSupported()

                .buildTests();
    }

    private List<TableFactory> customerOrderTables() {
        return List.of(
                databricksSchema -> databricksSchema
                        .createTable("customers", "CID", "INT", "NAME", "STRING", "COUNTRY", "STRING")
                        .bulkInsert(Stream.of(List.of(1, "c1", "a"), List.of(2, "c2", "a"), List.of(3, "c3", "b"))), // ,
                databricksSchema -> databricksSchema
                        .createTable("orders", "OID", "INT", "CUST_ID", "INT", "DATE", "DATE")
                        .bulkInsert(Stream.of(List.of(10, 1, "2024-09-01"), List.of(11, 2, "2024-09-02"),
                                List.of(12, 2, "2024-09-03"), List.of(13, 4, "2024-09-04"))));
    }

    @TestFactory
    Stream<DynamicNode> join() {
        return testSetup.pushdownTest(customerOrderTables())

                .capability("JOIN_CONDITION_ALL")
                .query("select NAME, COUNTRY, OID from $customers, $orders order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c1", "a", 11).row("c1", "a", 12).row("c1", "a", 13) //
                        .row("c2", "a", 10).row("c2", "a", 11).row("c2", "a", 12).row("c2", "a", 13) //
                        .row("c3", "b", 10).row("c3", "b", 11).row("c3", "b", 12).row("c3", "b", 13) //
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .pushdownNotSupported()

                .capability("JOIN_CONDITION_EQUI")
                .query("select NAME, COUNTRY, OID from $customers join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT").row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("INNER JOIN"),
                        containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)")))

                .capability("JOIN_TYPE_INNER")
                .query("select NAME, COUNTRY, OID from $customers inner join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT").row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("INNER JOIN"),
                        containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)")))

                .capability("JOIN_TYPE_FULL_OUTER")
                .query("select NAME, COUNTRY, OID from $customers full outer join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12).row("c3", "b", null)
                        .row(null, null, 13).matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("FULL OUTER JOIN"),
                        containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)")))

                .capability("JOIN_TYPE_LEFT_OUTER")
                .query("select NAME, COUNTRY, OID from $customers left outer join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12).row("c3", "b", null)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("LEFT OUTER JOIN"),
                        containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)")))

                .capability("JOIN_TYPE_RIGHT_OUTER")
                .query("select NAME, COUNTRY, OID from $customers right outer join $orders on CID=CUST_ID order by name, oid")
                .expect(table("VARCHAR", "VARCHAR", "BIGINT") //
                        .row("c1", "a", 10).row("c2", "a", 11).row("c2", "a", 12).row(null, null, 13)
                        .matches(TypeMatchMode.NO_JAVA_TYPE_CHECK))
                .expectPushdown(allOf(containsString("RIGHT OUTER JOIN"),
                        containsString("ON (`customers`.`CID` = `orders`.`CUST_ID`)")))

                .buildTests();
    }

    @TestFactory
    Stream<DynamicNode> literals() {
        return testSetup
                .pushdownTest(databricksSchema -> databricksSchema.createTable("tab", "VAL", "INT")
                        .bulkInsert(Stream.of(List.of(1L))))

                .capability("LITERAL_BOOL").query("select true, false, VAL from $tab")
                .expect(table("BOOLEAN", "BOOLEAN", "BIGINT").row(true, false, 1L).matches())
                .expectPushdown(startsWith("SELECT true, false, `tab`.`VAL` FROM"))

                .capability("LITERAL_DATE").query("select DATE '2024-09-18', VAL from $tab")
                .expect(table("DATE", "BIGINT").row(java.sql.Date.valueOf("2024-09-18"), 1L).matches())
                .expectPushdown(startsWith("SELECT DATE '2024-09-18', `tab`.`VAL` FROM"))

                .capability("LITERAL_DOUBLE").query("select 1.23456E-32, VAL from $tab")
                .expect(table("DOUBLE PRECISION", "BIGINT").row(1.23456E-32, 1L).matches())
                .expectPushdown(startsWith("SELECT 1.2345600000000001e-32, `tab`.`VAL` FROM"))

                .capability("LITERAL_EXACTNUMERIC").query("select -123.456, VAL from $tab")
                .expect(table("DECIMAL", "BIGINT").row(new BigDecimal("-123.456"), 1L).matches())
                .expectPushdown(startsWith("SELECT -123.456, `tab`.`VAL` FROM"))

                .capability("LITERAL_INTERVAL").info("year to month")
                .query("select INTERVAL '13-03' YEAR TO MONTH, VAL from $tab")
                .expect(table("INTERVAL YEAR TO MONTH", "BIGINT").row("+13-03", 1L).matches()).pushdownNotSupported()

                .capability("LITERAL_INTERVAL").info("day to second")
                .query("select INTERVAL '1 12:00:30.123' DAY TO SECOND, VAL from $tab")
                .expect(table("INTERVAL DAY TO SECOND", "BIGINT").row("+01 12:00:30.123", 1L).matches())
                .pushdownNotSupported()

                .capability("LITERAL_NULL").query("select NULL, VAL from $tab")
                .expect(table("BOOLEAN", "BIGINT").row(null, 1L).matches())
                .expectPushdown(startsWith("SELECT null, `tab`.`VAL` FROM"))

                .capability("LITERAL_STRING").query("select 'literal', VAL from $tab")
                .expect(table("CHAR", "BIGINT").row("literal", 1L).matches())
                .expectPushdown(startsWith("SELECT 'literal', `tab`.`VAL` FROM"))

                .capability("LITERAL_TIMESTAMP").query("select TIMESTAMP '2007-03-31 12:59:30.123', VAL from $tab")
                .expect(table("TIMESTAMP", "BIGINT").row(Timestamp.valueOf("2007-03-31 12:59:30.123"), 1L).matches())
                .expectPushdown(startsWith("SELECT TIMESTAMP '2007-03-31 12:59:30.123', `tab`.`VAL` FROM"))

                .capability("LITERAL_TIMESTAMP_UTC").query("select TIMESTAMP '2007-03-31 12:59:30.123', VAL from $tab")
                .expect(table("TIMESTAMP", "BIGINT").row(Timestamp.valueOf("2007-03-31 12:59:30.123"), 1L).matches())
                .expectPushdown(startsWith("SELECT TIMESTAMP '2007-03-31 12:59:30.123', `tab`.`VAL` FROM"))

                .buildTests();
    }

    @TestFactory
    Stream<DynamicNode> predicates() {
        return testSetup
                .pushdownTest(databricksSchema -> databricksSchema.createTable("tab", "VAL", "INT")
                        .bulkInsert(Stream.of(List.of(1L))))

                .capability("FN_PRED_AND").info("evaluate constants").query("select true and false from $tab")
                .expect(table("BOOLEAN").row(false).matches()).expectPushdown(startsWith("SELECT false FROM"))

                .capability("FN_PRED_AND").info("pushdown").query("select VAL>=1 and VAL!=0 from $tab")
                .expect(table("BOOLEAN").row(true).matches())
                .expectPushdown(startsWith("SELECT ((1 <= `tab`.`VAL`) AND (`tab`.`VAL` <> 0)) FROM"))

                .capability("FN_PRED_OR").query("select VAL=1 or VAL>1 from $tab")
                .expect(table("BOOLEAN").row(true).matches())
                .expectPushdown(startsWith("SELECT ((`tab`.`VAL` = 1) OR (1 < `tab`.`VAL`)) FROM"))

                .capability("FN_PRED_BETWEEN").query("select VAL between 0 and 2 from $tab")
                .expect(table("BOOLEAN").row(true).matches())
                .expectPushdown(startsWith("SELECT (`tab`.`VAL` BETWEEN 0 AND 2) FROM"))

                .capability("FN_PRED_EQUAL").query("select VAL=1 from $tab")
                .expect(table("BOOLEAN").row(true).matches())
                .expectPushdown(startsWith("SELECT (`tab`.`VAL` = 1) FROM"))

                .capability("FN_PRED_NOTEQUAL").query("select VAL!=0 from $tab")
                .expect(table("BOOLEAN").row(true).matches())
                .expectPushdown(startsWith("SELECT (`tab`.`VAL` <> 0) FROM"))

                .buildTests();
    }
}
