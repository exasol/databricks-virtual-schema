package com.exasol.adapter.databricks;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.itsallcode.matcher.auto.AutoMatcher;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.exasol.ExasolVirtualSchema;
import com.exasol.adapter.databricks.fixture.exasol.MetadataDao.ExaColumn;

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
        final ExasolVirtualSchema vs = testSetup.exasol().createVirtualSchema("system", "information_schema",
                emptyMap());
        testSetup.exasol().assertions().virtualSchemaExists(vs);
    }

    @Test
    void alterVirtualSchemaSetProperty() {
        final DatabricksSchema databricksSchema1 = testSetup.databricks().createSchema();
        final DatabricksSchema databricksSchema2 = testSetup.databricks().createSchema();
        databricksSchema1.createTable("tab1", "col1", "VARCHAR(5)");
        databricksSchema2.createTable("tab2", "col2", "INTEGER");
        final ExasolVirtualSchema vs = testSetup.exasol().createVirtualSchema(databricksSchema1);
        assertThat(testSetup.exasol().metadata().getVirtualColumns(vs),
                AutoMatcher.equalTo(List.of(new ExaColumn("tab1", "col1", "VARCHAR(5) UTF8", 5L, null, null))));

        vs.setProperties(Map.of("SCHEMA_NAME", databricksSchema2.getName()));

        assertThat(testSetup.exasol().metadata().getVirtualColumns(vs),
                AutoMatcher.equalTo(List.of(new ExaColumn("tab2", "col2", "DECIMAL(10,0)", 10L, 10L, 0L))));
    }

    @Test
    void alterVirtualSchemaSetPropertyFailsForMissingSchema() {
        final DatabricksSchema databricksSchema1 = testSetup.databricks().createSchema();
        databricksSchema1.createTable("tab1", "col1", "VARCHAR(5)");
        final ExasolVirtualSchema vs = testSetup.exasol().createVirtualSchema(databricksSchema1);
        assertThat(testSetup.exasol().metadata().getVirtualColumns(vs),
                AutoMatcher.equalTo(List.of(new ExaColumn("tab1", "col1", "VARCHAR(5) UTF8", 5L, null, null))));

        final Map<String, String> newProperties = Map.of("SCHEMA_NAME", "missing-schema");
        final RuntimeException exception = assertThrows(RuntimeException.class, () -> vs.setProperties(newProperties));
        assertThat(exception.getMessage(), containsString("failed with status 404"));
    }

    @Test
    void alterVirtualSchemaRefresh() {
        final DatabricksSchema databricksSchema = testSetup.databricks().createSchema();
        databricksSchema.createTable("tab1", "col1", "VARCHAR(5)");
        final ExasolVirtualSchema vs = testSetup.exasol().createVirtualSchema(databricksSchema);
        assertThat(testSetup.exasol().metadata().getVirtualColumns(vs),
                AutoMatcher.equalTo(List.of(new ExaColumn("tab1", "col1", "VARCHAR(5) UTF8", 5L, null, null))));

        databricksSchema.createTable("tab2", "col2", "BIGINT");
        vs.refresh();

        assertThat(testSetup.exasol().metadata().getVirtualColumns(vs),
                AutoMatcher.equalTo(List.of(new ExaColumn("tab1", "col1", "VARCHAR(5) UTF8", 5L, null, null),
                        new ExaColumn("tab2", "col2", "DECIMAL(19,0)", 19L, 19L, 0L))));
    }
}
