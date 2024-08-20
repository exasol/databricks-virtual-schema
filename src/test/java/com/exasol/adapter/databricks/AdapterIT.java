package com.exasol.adapter.databricks;

import org.junit.jupiter.api.Test;

class AdapterIT extends AbstractIntegrationTestBase {

    @Test
    void test() {
        testSetup().createVirtualSchema();
    }
}
