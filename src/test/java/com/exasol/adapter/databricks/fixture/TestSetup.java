package com.exasol.adapter.databricks.fixture;

import java.util.logging.Logger;

import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture;
import com.exasol.adapter.databricks.fixture.exasol.ExasolFixture;

public class TestSetup implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(TestSetup.class.getName());

    private final ExasolFixture exasolFixture;
    private final DatabricksFixture databricksFixture;

    private TestSetup(final ExasolFixture exasolFixture, final DatabricksFixture databricksFixture) {
        this.exasolFixture = exasolFixture;
        this.databricksFixture = databricksFixture;
    }

    public static TestSetup start() {
        final TestConfig config = TestConfig.read();
        final DatabricksFixture databricks = DatabricksFixture.create(config);
        final ExasolFixture exasol = ExasolFixture.start(config, databricks);
        return new TestSetup(exasol, databricks);
    }

    public DatabricksFixture databricks() {
        return this.databricksFixture;
    }

    public ExasolFixture exasol() {
        return this.exasolFixture;
    }

    public MultiTestSetup datatypeMappingTest() {
        return new MultiTestSetup(this);
    }

    public void cleanupAfterTest() {
        this.exasolFixture.cleanupAfterTest();
    }

    @Override
    public void close() {
        this.exasolFixture.close();
        this.databricksFixture.close();
    }
}
