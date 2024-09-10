package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;

import java.sql.ResultSet;
import java.util.stream.Stream;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

class PushdownQueryIT extends AbstractIntegrationTestBase {

    private static Table table;
    private static VirtualSchema virtualSchema;

    @BeforeAll
    static void createVirtualSchema() {
        final DatabricksSchema databricksSchema = testSetup.databricks().createSchema();
        table = databricksSchema.createTable("tab", "ID", "INT", "NAME", "STRING").insert(1, "a").insert(2, "b")
                .insert(3, "c");
        virtualSchema = testSetup.exasol().createVirtualSchema(databricksSchema);
    }

    @AfterEach
    @Override
    void cleanupAfterTest() {
        // Cleanup not needed
    }

    static Stream<Arguments> pushdownQueryTestData() {
        return Stream.of(
                test("select *", "SELECT * FROM $VIRTUAL_TABLE",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matchesInAnyOrder()),
                test("SELECTLIST_PROJECTION", "SELECT id, name FROM $VIRTUAL_TABLE",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matchesInAnyOrder()),
                test("SELECTLIST_PROJECTION - single column", "SELECT id FROM $VIRTUAL_TABLE order by id",
                        table("DECIMAL").row(1).row(2).row(3).matches()),
                test("ORDER_BY_COLUMN", "SELECT id, name FROM $VIRTUAL_TABLE order by id",
                        table("DECIMAL", "VARCHAR").row(1, "a").row(2, "b").row(3, "c").matches()),
                test("SELECTLIST_EXPRESSIONS", "SELECT id*2, 'name: '||name FROM $VIRTUAL_TABLE order by id",
                        table("DECIMAL", "VARCHAR").row(2, "name: a").row(4, "name: b").row(6, "name: c").matches())

        );
    }

    @ParameterizedTest(name = "pushdown query capability {0}")
    @MethodSource("pushdownQueryTestData")
    void pushdown(final PushdownQueryTestData testData) {
        testSetup.exasol().assertions().query(testData.getQuery(virtualSchema, table),
                testData.expectedResultMatcher());
    }

    private static Arguments test(final String testName, final String query,
            final Matcher<ResultSet> expectedResultMatcher) {
        return Arguments.of(new PushdownQueryTestData(testName, query, expectedResultMatcher));
    }

    record PushdownQueryTestData(String capability, String query, Matcher<ResultSet> expectedResultMatcher) {
        String getQuery(final VirtualSchema virtualSchema, final Table table) {
            return query.replace("$VIRTUAL_TABLE", virtualTableName(virtualSchema, table));
        }

        private static String virtualTableName(final VirtualSchema virtualSchema, final Table databricksTable) {
            return String.format("\"%s\".\"%s\"", virtualSchema.getName(), databricksTable.getName());
        }

        @Override
        public final String toString() {
            return this.capability();
        }
    }
}
