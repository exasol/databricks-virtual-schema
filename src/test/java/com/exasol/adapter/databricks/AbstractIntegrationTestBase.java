package com.exasol.adapter.databricks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.exasol.adapter.databricks.fixture.TestSetup;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

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

    protected VirtualSchema createVirtualSchema(final String databricksCatalog, final String databricksSchema) {
        return testSetup.exasol().createVirtualSchema(databricksCatalog, databricksSchema);
    }

    protected void assertCreateVirtualSchemaFails(final String databricksCatalog, final String databricksSchema,
            final Matcher<String> expectedErrorMessage) {
        final Exception exception = assertThrows(Exception.class,
                () -> createVirtualSchema(databricksCatalog, databricksSchema));
        assertThat(exception.getMessage(),
                allOf(startsWith("E-TDBJ-13: Failed to write to object"), expectedErrorMessage));
    }
}
