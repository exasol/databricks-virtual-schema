package com.exasol.adapter.databricks;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

class AbstractIntegrationTestBase {

    protected static TestSetup testSetup;

    @BeforeAll
    static void beforeAll() {
        testSetup = TestSetup.start();
        testSetup.buildAdapter();
    }

    @AfterAll
    static void afterAll() {
        testSetup.close();
        testSetup = null;
    }
}
