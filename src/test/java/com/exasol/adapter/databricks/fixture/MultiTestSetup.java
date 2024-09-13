package com.exasol.adapter.databricks.fixture;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.*;
import java.util.logging.Logger;

import org.itsallcode.matcher.auto.AutoMatcher;
import org.junit.jupiter.api.function.Executable;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.exasol.MetadataDao.ExaColumn;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

/**
 * This class provides a fluent API for setting up and verifying multiple column tests. The goal is to speed up tests by
 * reducing the number of Databricks tables to create. This creates a single Databricks table with multiple columns and
 * verifies the metadata of all columns at once.
 */
public class MultiTestSetup {
    private static final Logger LOG = Logger.getLogger(MultiTestSetup.class.getName());

    private final TestSetup testSetup;
    private final List<ColumnTypeTest> columnTests = new ArrayList<>();

    MultiTestSetup(final TestSetup testSetup) {
        this.testSetup = testSetup;
    }

    public MultiTestSetup add(final String databricksType, final String expectedExasolType,
            final long expectedMaxSize) {
        return addTypeTest(databricksType, expectedExasolType, expectedMaxSize, null, null);
    }

    public MultiTestSetup addIntervalYearToMonth(final String databricksType) {
        // Mapping of Databricks interval types to precision is not clear, using maximum value.
        return addTypeTest(databricksType, "INTERVAL YEAR(9) TO MONTH", 13L, null, null);
    }

    public MultiTestSetup addIntervalDayToSecond(final String databricksType) {
        // Mapping of Databricks interval types to precision and fraction is not clear, using maximum values.
        return addTypeTest(databricksType, "INTERVAL DAY(9) TO SECOND(9)", 29L, null, null);
    }

    public MultiTestSetup addDecimal(final String databricksType, final long expectedPrecision,
            final long expectedScale) {
        return addTypeTest(databricksType, String.format("DECIMAL(%d,%d)", expectedPrecision, expectedScale),
                expectedPrecision, expectedPrecision, expectedScale);
    }

    private MultiTestSetup addTypeTest(final String databricksType, final String expectedExasolType,
            final Long expectedMaxSize, final Long expectedPrecision, final Long expectedScale) {
        final int colId = this.columnTests.size();
        final String columnName = String.format("col%02d_%s", colId, sanitizeColumnName(databricksType));
        final ColumnTypeTest columnTest = new ColumnTypeTest(columnName, databricksType,
                new ExpectedExasolType(expectedExasolType, expectedMaxSize, expectedPrecision, expectedScale),
                emptyList(), emptyList());
        this.columnTests.add(columnTest);
        return this;
    }

    public ValueMappingBuilder addValueTest(final String databricksType) {
        return new ValueMappingBuilder(this, databricksType);
    }

    public static class ValueMappingBuilder {
        private final MultiTestSetup multiTestSetup;
        private final String databricksType;
        private final List<Object> databricksValues = new ArrayList<>();
        private final List<Object> expectedExasolValues = new ArrayList<>();

        private ValueMappingBuilder(final MultiTestSetup multiTestSetup, final String databricksType) {
            this.multiTestSetup = multiTestSetup;
            this.databricksType = databricksType;
        }

        public ValueMappingBuilder valueMapped(final Object databricksValue, final Object expectedExasolValue) {
            this.databricksValues.add(databricksValue);
            this.expectedExasolValues.add(expectedExasolValue);
            return this;
        }

        public MultiTestSetup done() {
            return multiTestSetup.addValueTest(this);
        }
    }

    private MultiTestSetup addValueTest(final ValueMappingBuilder builder) {
        final int colId = this.columnTests.size();
        final String columnName = String.format("col%02d_%s", colId, sanitizeColumnName(builder.databricksType));
        final ColumnTypeTest columnTest = new ColumnTypeTest(columnName, builder.databricksType, null,
                builder.databricksValues, builder.expectedExasolValues);
        this.columnTests.add(columnTest);
        return this;
    }

    private String sanitizeColumnName(String value) {
        for (final String specialChar : List.of(" ", ",", "'", "(", ")")) {
            value = value.replace(specialChar, "_");
        }
        return value;
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
        final List<List<Object>> actualColumnValues = testSetup.exasol().metadata().getVirtualColumnValues(vs,
                databricksTable);

        final Collection<Executable> columnAssertions = new ArrayList<>();
        for (int i = 0; i < this.columnTests.size(); i++) {
            final ExaColumn actual = actualColumns.get(i);
            final List<Object> rowValues = actualColumnValues.get(i);
            columnAssertions.add(this.columnTests.get(i).assertion(actual, rowValues));
        }
        assertAll(columnAssertions);
    }

    private Table createDatabricksTable(final DatabricksSchema databricksSchema) {
        final List<String> columnNames = this.columnTests.stream().map(ColumnTypeTest::columnName).toList();
        final List<String> columnTypes = this.columnTests.stream().map(ColumnTypeTest::databricksType).toList();
        LOG.fine("Creating Databricks table with columns " + columnNames + " and types " + columnTypes);
        final Table table = databricksSchema.createTable("tab", columnNames, columnTypes);
        getDatabricksRowValues().forEach(table::insert);
        return table;
    }

    private List<Object[]> getDatabricksRowValues() {
        final int rowCount = this.columnTests.stream().map(ColumnTypeTest::databricksValues).mapToInt(List::size).max()
                .orElseThrow();
        final List<Object[]> rowValues = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            final int rowIndex = i;
            rowValues.add(this.columnTests.stream() //
                    .map(ColumnTypeTest::databricksValues) //
                    .map(values -> values.size() > rowIndex ? values.get(rowIndex) : null) //
                    .toArray());
        }
        return rowValues;
    }

    private static record ExpectedExasolType(String exasolType, Long maxSize, Long precision, Long scale) {
    }

    private static record ColumnTypeTest(String columnName, String databricksType, ExpectedExasolType expectedType,
            List<Object> databricksValues, List<Object> expectedValues) {

        Executable assertion(final ExaColumn actual, final List<Object> actualRowValues) {
            return () -> {
                assertAll(() -> assertType(actual), //
                        () -> assertValues(actualRowValues));
            };
        }

        private void assertType(final ExaColumn actual) {
            if (expectedType != null) {
                assertThat(description(), actual, AutoMatcher.equalTo(getExpectedExaColumn()));
            }
        }

        private void assertValues(final List<Object> actualRowValues) {
            if (expectedValues != null) {
                assertThat(description(), actualRowValues, AutoMatcher.equalTo(expectedValues));
            }
        }

        private String description() {
            return String.format("Column %s: databricks type %s", this.columnName, this.databricksType);
        }

        private ExaColumn getExpectedExaColumn() {
            return new ExaColumn(columnName, expectedType.exasolType, expectedType.maxSize, expectedType.precision,
                    expectedType.scale);
        }
    }
}
