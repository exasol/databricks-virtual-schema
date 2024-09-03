package com.exasol.adapter.databricks;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

class AdapterIT extends AbstractIntegrationTestBase {

    @Test
    void createFailsForMissingArguments() {
        assertCreateVirtualSchemaFails(null, null, containsString("F-VSDAB-1: Property 'CATALOG_NAME' is missing"));
    }

    @Test
    void createFailsForNonExistingDatabricksCatalog() {
        assertCreateVirtualSchemaFails("no-such-catalog", "schema",
                allOf(containsString("E-VSDAB-5: HTTP request for URL 'https://"),
                        containsString("Catalog 'no-such-catalog' does not exist")));
    }

    @Test
    void createFailsForNonExistingDatabricksSchema() {
        assertCreateVirtualSchemaFails("system", "no-such-schema",
                allOf(containsString("E-VSDAB-5: HTTP request for URL 'https://"),
                        containsString("Schema 'system.no-such-schema' does not exist.")));
    }

    @Test
    void schemaMetadataAvailable() {
        final VirtualSchema vs = testSetup.exasol().createVirtualSchema("system", "information_schema");
        testSetup.exasol().assertions().virtualSchemaExists(vs);
    }

    @Test
    void dataTypeMapping() {
        // https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
        testSetup.datatypeMappingTest() //
                .add("string", "VARCHAR(2000000) UTF8", 2000000L)
                .add("string not null", "VARCHAR(2000000) UTF8", 2000000L)
                .add("string generated always as ('gen')", "VARCHAR(2000000) UTF8", 2000000L)
                .add("string comment 'my column'", "VARCHAR(2000000) UTF8", 2000000L) //

                .addDecimal("TINYINT", 3, 0) //
                .addDecimal("SMALLINT", 5, 0) //
                .addDecimal("INT", 10, 0) //
                .addDecimal("BIGINT", 19, 0) //
                .addDecimal("BIGINT generated by default as identity", 19, 0) //

                .addDecimal("DECIMAL(1,0)", 1, 0) //
                .addDecimal("DECIMAL(1,1)", 1, 1) //
                .addDecimal("DECIMAL(4,2)", 4, 2) //
                .addDecimal("DECIMAL(10)", 10, 0) //
                .addDecimal("DECIMAL(36)", 36, 0) //
                .addDecimal("DECIMAL(36,36)", 36, 36) //

                .add("FLOAT", "DOUBLE", 64) //
                .add("DOUBLE", "DOUBLE", 64) //

                .add("BOOLEAN", "BOOLEAN", 1) //

                .add("TIMESTAMP", "TIMESTAMP(3) WITH LOCAL TIME ZONE", 29) //
                .add("TIMESTAMP_NTZ", "TIMESTAMP(3)", 29) //

                .addIntervalYearToMonth("INTERVAL YEAR") //
                .addIntervalYearToMonth("INTERVAL YEAR TO MONTH") //
                .addIntervalYearToMonth("INTERVAL MONTH") //

                .addIntervalDayToSecond("INTERVAL DAY") //
                .addIntervalDayToSecond("INTERVAL DAY TO HOUR") //
                .addIntervalDayToSecond("INTERVAL DAY TO MINUTE") //
                .addIntervalDayToSecond("INTERVAL DAY TO SECOND") //
                .addIntervalDayToSecond("INTERVAL HOUR") //
                .addIntervalDayToSecond("INTERVAL HOUR TO MINUTE") //
                .addIntervalDayToSecond("INTERVAL HOUR TO SECOND") //
                .addIntervalDayToSecond("INTERVAL MINUTE") //
                .addIntervalDayToSecond("INTERVAL MINUTE TO SECOND") //
                .addIntervalDayToSecond("INTERVAL SECOND") //
                .verify();
    }

    @ParameterizedTest
    @CsvSource(delimiterString = ";", value = { "ARRAY<INT>; ARRAY", "MAP<INT,STRING>; MAP",
            "STRUCT<id:INT,name:STRING>; STRUCT", "VARIANT; VARIANT", "BINARY; BINARY" })
    void unsupportedDataTypes(final String databricksType, final String typeInErrorMessage) {
        final DatabricksSchema databricksSchema = testSetup.databricks().createSchema();
        databricksSchema.createTable("tab", "col", databricksType + " COMMENT 'my column'");
        testSetup.exasol().assertions().assertVirtualSchemaFails(databricksSchema,
                equalTo(String.format(
                        """
                                E-VSDAB-8: Exasol does not support Databricks data type '%s' of column 'col' at position 0 with comment 'my column'

                                Mitigations:

                                * Please remove the column or change the data type.""",
                        typeInErrorMessage)));
    }

    @Test
    void unsupportedDecimalPrecision() {
        final DatabricksSchema databricksSchema = testSetup.databricks().createSchema();
        databricksSchema.createTable("tab", "col", "DECIMAL(38) COMMENT 'my column'");
        testSetup.exasol().assertions().assertVirtualSchemaFails(databricksSchema,
                equalTo("""
                        E-VSDAB-11: Unsupported decimal precision 'decimal(38,0)' for column 'col' at position 0 (comment: 'my column'), Exasol supports a maximum precision of 36.

                        Mitigations:

                        * Please remove the column or change the data type."""));
    }

    @Test
    void pushdownQuery() {
        final DatabricksSchema databricksSchema = testSetup.databricks().createSchema();
        final Table table = databricksSchema.createTable("tab", "ID", "INT", "NAME", "STRING").insert(1, "a")
                .insert(2, "b").insert(3, "c");
        final VirtualSchema vs = testSetup.exasol().createVirtualSchema(databricksSchema);
        testSetup.exasol().assertions().query("SELECT * FROM " + virtualTableName(vs, table) + " ORDER BY ID",
                table().row(1, "a").row(2, "b").row(3, "c").matches());
    }

    private String virtualTableName(final VirtualSchema virtualSchema, final Table databricksTable) {
        return String.format("\"%s\".\"%s\"", virtualSchema.getName(), databricksTable.getName());
    }
}
