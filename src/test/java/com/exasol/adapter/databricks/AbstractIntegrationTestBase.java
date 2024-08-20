package com.exasol.adapter.databricks;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

class AbstractIntegrationTestBase {

    private static TestSetup testSetup;

    @BeforeAll
    static void beforeAll() {
        testSetup = TestSetup.start();
    }

    protected TestSetup testSetup() {
        assertNotNull(testSetup, "Test setup not available yet. beforeAll() must run before.");
        return testSetup;
    }

    @AfterAll
    static void afterAll() {
        testSetup.close();
        testSetup = null;
    }
}
