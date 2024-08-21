package com.exasol.adapter.databricks;

import org.junit.jupiter.api.Test;

import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

class AdapterIT extends AbstractIntegrationTestBase {

    @Test
    void test() {
        final VirtualSchema vs = testSetup.createVirtualSchema();
        testSetup.assertions().virtualSchemaExists(vs);
    }
}
