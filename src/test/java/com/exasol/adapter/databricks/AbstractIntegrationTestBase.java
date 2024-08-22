package com.exasol.adapter.databricks;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.exasol.adapter.databricks.fixture.TestSetup;

class AbstractIntegrationTestBase {

    protected static TestSetup testSetup;

    @BeforeAll
    static void beforeAll() {
        testSetup = TestSetup.start();
        testSetup.exasol().buildAdapter();
    }

    @AfterAll
    static void afterAll() {
        if (testSetup != null) {
            testSetup.close();
            testSetup = null;
        }
    }
}
