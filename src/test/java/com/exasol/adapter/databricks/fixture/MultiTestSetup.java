package com.exasol.adapter.databricks.fixture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.*;
import java.util.logging.Logger;

import org.junit.jupiter.api.function.Executable;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.exasol.MetadataDao.ExaColumn;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

public class MultiTestSetup {
    private static final Logger LOG = Logger.getLogger(MultiTestSetup.class.getName());

    private final TestSetup testSetup;
    private final List<ColumnTest> columnTests = new ArrayList<>();

    MultiTestSetup(final TestSetup testSetup) {
        this.testSetup = testSetup;
    }

    public MultiTestSetup add(final String databricksType, final String expectedExasolType,
            final long expectedMaxSize) {
        return add(databricksType, expectedExasolType, expectedMaxSize, null, null);
    }

    private MultiTestSetup add(final String databricksType, final String expectedExasolType, final Long expectedMaxSize,
            final Long expectedPrecision, final Long expectedScale) {
        final int colId = this.columnTests.size();
        final ColumnTest columnTest = new ColumnTest("col" + colId, databricksType, expectedExasolType, expectedMaxSize,
                expectedPrecision, expectedScale);
        this.columnTests.add(columnTest);
        return this;
    }

    public void verify() {
        final DatabricksSchema databricksSchema = this.testSetup.databricks().createSchema();
        final Table databricksTable = createDatabricksTable(databricksSchema);
        final VirtualSchema vs = this.testSetup.exasol().createVirtualSchema(databricksSchema);
        verifyColumnMetadata(databricksTable, vs);
    }

    private void verifyColumnMetadata(final Table databricksTable, final VirtualSchema vs) {
        final List<ExaColumn> actualColumns = testSetup.exasol().metadata().getVirtualColumns(vs, databricksTable);
        assertThat("column count - probably some column types are not supported", actualColumns.size(),
                equalTo(this.columnTests.size()));
        final Collection<Executable> columnAssertions = new ArrayList<>();
        for (int i = 0; i < this.columnTests.size(); i++) {
            final ExaColumn actual = actualColumns.get(i);
            columnAssertions.add(this.columnTests.get(i).assertion(actual));
        }
        assertAll(columnAssertions);
    }

    private Table createDatabricksTable(final DatabricksSchema databricksSchema) {
        final List<String> columnNames = this.columnTests.stream().map(ColumnTest::columnName).toList();
        final List<String> columnTypes = this.columnTests.stream().map(ColumnTest::databricksType).toList();
        LOG.fine("Creating Databricks table with columns " + columnNames + " and types " + columnTypes);
        return databricksSchema.createTable("tab", columnNames, columnTypes);
    }

    private static record ColumnTest(String columnName, String databricksType, String exasolType, Long maxSize,
            Long precision, Long scale) {

        private ExaColumn getExpected() {
            return new ExaColumn(columnName, exasolType, maxSize, precision, scale);
        }

        Executable assertion(final ExaColumn actual) {
            return () -> {
                final String reason = String.format("Column %s: databricks type %s - exasol type %s", this.columnName,
                        this.databricksType, this.exasolType);
                assertThat(reason, actual, equalTo(getExpected()));
            };
        }
    }
}
