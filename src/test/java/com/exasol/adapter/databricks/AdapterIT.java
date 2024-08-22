package com.exasol.adapter.databricks;

import org.junit.jupiter.api.Test;

import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

class AdapterIT extends AbstractIntegrationTestBase {

    @Test
    void test() {
        final VirtualSchema vs = testSetup.createVirtualSchema();
        testSetup.assertions().virtualSchemaExists(vs);
    }

    @Test
    void testDb() {
        final long timestamp = System.currentTimeMillis();
        testSetup.databricks().createCatalog("db-vs-test-" + timestamp).createSchema("db-vs-test-schema-" + timestamp)
                .createTable("tab", "col", "varchar(10)");
    }
}
