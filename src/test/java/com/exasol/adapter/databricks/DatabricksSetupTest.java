package com.exasol.adapter.databricks;

import org.junit.jupiter.api.*;

import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture;
import com.exasol.adapter.databricks.fixture.TestConfig;

class DatabricksSetupTest {

    private DatabricksFixture databricks;

    @BeforeEach
    void setUp() {
        databricks = DatabricksFixture.create(TestConfig.read());
    }

    @AfterEach
    void tearDown() {
        databricks.close();
    }

    @Test
    void createCatalog() {
        final long timestamp = System.currentTimeMillis();
        databricks.createCatalog("db-vs-test-" + timestamp).createSchema("db-vs-test-schema-" + timestamp)
                .createTable("tab", "col", "varchar(10)").insert("my content");

        System.out.println("stop");
    }
}
