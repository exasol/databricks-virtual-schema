package com.exasol.adapter.databricks.fixture;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.itsallcode.matcher.auto.AutoMatcher;
import org.junit.jupiter.api.*;

import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture.AuthMode;
import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.exasol.ExasolFixture;
import com.exasol.adapter.databricks.fixture.exasol.ExasolVirtualSchema;
import com.exasol.adapter.databricks.fixture.exasol.MetadataDao.ExaColumn;
import com.exasol.adapter.databricks.fixture.exasol.MetadataDao.TableData;
import com.exasol.dbbuilder.dialects.Table;

/**
 * This class provides a fluent API for setting up and verifying multiple column tests. The goal is to speed up tests by
 * reducing the number of Databricks tables to create. This creates a single Databricks table with multiple columns and
 * verifies the metadata of all columns at once.
 */
public class MultiTestSetup {
    private static final String ROW_ORDER_COLUMN_NAME = "row_num";
    private static final Logger LOG = Logger.getLogger(MultiTestSetup.class.getName());

    private final TestSetup testSetup;
    private final List<ColumnTypeTest> columnTests = new ArrayList<>();

    MultiTestSetup(final TestSetup testSetup) {
        this.testSetup = testSetup;
    }

    @Deprecated
    public MultiTestSetup add(final String databricksType, final String expectedExasolType,
            final long expectedMaxSize) {
        return addTypeTest(databricksType, expectedExasolType, expectedMaxSize, null, null);
    }

    @Deprecated
    public MultiTestSetup addIntervalYearToMonth(final String databricksType) {
        // Mapping of Databricks interval types to precision is not clear, using maximum value.
        return addTypeTest(databricksType, "INTERVAL YEAR(9) TO MONTH", 13L, null, null);
    }

    @Deprecated
    public MultiTestSetup addIntervalDayToSecond(final String databricksType) {
        // Mapping of Databricks interval types to precision and fraction is not clear, using maximum values.
        return addTypeTest(databricksType, "INTERVAL DAY(9) TO SECOND(9)", 29L, null, null);
    }

    @Deprecated
    public MultiTestSetup addDecimal(final String databricksType, final long expectedPrecision,
            final long expectedScale) {
        return addTypeTest(databricksType, String.format("DECIMAL(%d,%d)", expectedPrecision, expectedScale),
                expectedPrecision, expectedPrecision, expectedScale);
    }

    private MultiTestSetup addTypeTest(final String databricksType, final String expectedExasolType,
            final Long expectedMaxSize, final Long expectedPrecision, final Long expectedScale) {
        final int colId = this.columnTests.size();
        final String columnName = String.format("col%02d_%s", colId, ExasolFixture.sanitizeExasolId(databricksType));
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
        private ExpectedExasolType expectedExasolType;

        private ValueMappingBuilder(final MultiTestSetup multiTestSetup, final String databricksType) {
            this.multiTestSetup = multiTestSetup;
            this.databricksType = databricksType;
        }

        public ValueMappingBuilder expectType(final String expectedExasolType, final long expectedMaxSize) {
            return expectedExasolType(new ExpectedExasolType(expectedExasolType, expectedMaxSize, null, null));
        }

        public ValueMappingBuilder expectVarchar() {
            return expectType("VARCHAR(2000000) UTF8", 2000000);
        }

        public ValueMappingBuilder expectIntervalYearToMonth() {
            // Mapping of Databricks interval types to precision is not clear, using maximum value.
            return expectedExasolType(new ExpectedExasolType("INTERVAL YEAR(9) TO MONTH", 13L, null, null));
        }

        public ValueMappingBuilder expectIntervalDayToSecond() {
            // Mapping of Databricks interval types to precision and fraction is not clear, using maximum values.
            return expectedExasolType(new ExpectedExasolType("INTERVAL DAY(9) TO SECOND(9)", 29L, null, null));
        }

        public ValueMappingBuilder expectDecimal(final long expectedPrecision, final long expectedScale) {
            return expectedExasolType(
                    new ExpectedExasolType(String.format("DECIMAL(%d,%d)", expectedPrecision, expectedScale),
                            expectedPrecision, expectedPrecision, expectedScale));
        }

        public ValueMappingBuilder expectTimestamp(final long expectedPrecision) {
            return expectedExasolType(
                    new ExpectedExasolType(String.format("TIMESTAMP(%d)", expectedPrecision),
                            29L, expectedPrecision, null));
        }

        private ValueMappingBuilder expectedExasolType(final ExpectedExasolType type) {
            this.expectedExasolType = type;
            return this;
        }

        public ValueMappingBuilder nullValue() {
            return value(null);
        }

        public ValueMappingBuilder value(final Object value) {
            return value(value, value);
        }

        public ValueMappingBuilder timestamp(final String databricksValue, final String expectedExasolValue) {
            return value(databricksValue, Timestamp.valueOf(expectedExasolValue));
        }

        public ValueMappingBuilder date(final String databricksValue, final String expectedExasolValue) {
            return value(databricksValue, Date.valueOf(expectedExasolValue));
        }

        public ValueMappingBuilder value(final Object databricksValue, final Object expectedExasolValue) {
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
        final String columnName = String.format("col%02d_%s", colId,
                ExasolFixture.sanitizeExasolId(builder.databricksType));
        final ColumnTypeTest columnTest = new ColumnTypeTest(columnName, builder.databricksType,
                builder.expectedExasolType, builder.databricksValues, builder.expectedExasolValues);
        this.columnTests.add(columnTest);
        return this;
    }

    public Stream<DynamicNode> buildTests() {
        final DatabricksSchema databricksSchema = this.testSetup.databricks().createSchema();
        final Table databricksTable = createDatabricksTable(databricksSchema);
        final ExasolVirtualSchema vs = this.testSetup.exasol().createVirtualSchema(databricksSchema, AuthMode.TOKEN);
        return verifyColumnMetadata(databricksTable, vs);
    }

    private Stream<DynamicNode> verifyColumnMetadata(final Table databricksTable, final ExasolVirtualSchema vs) {
        final List<ExaColumn> actualColumns = testSetup.exasol().metadata().getVirtualColumns(vs, databricksTable);
        assertThat("column count - probably some column types are not supported", actualColumns.size(),
                equalTo(this.columnTests.size() + 1));
        final String query = "select * from " + vs.qualifyTableName(databricksTable) + " order by \""
                + ROW_ORDER_COLUMN_NAME.toUpperCase() + "\" asc";
        final TableData actualData = testSetup.exasol().metadata().getTableData(query);
        final List<DynamicNode> tests = new ArrayList<>();
        for (int i = 0; i < this.columnTests.size(); i++) {
            final ExaColumn actualType = actualColumns.get(i + 1);
            tests.add(this.columnTests.get(i).createTest(databricksTable, actualType, actualData));
        }
        return tests.stream();
    }

    private Table createDatabricksTable(final DatabricksSchema databricksSchema) {
        final List<String> columnNames = Stream
                .concat(Stream.of(ROW_ORDER_COLUMN_NAME), this.columnTests.stream().map(ColumnTypeTest::columnName))
                .toList();
        final List<String> columnTypes = Stream
                .concat(Stream.of("INTEGER"), this.columnTests.stream().map(ColumnTypeTest::databricksType)).toList();
        LOG.fine("Creating Databricks table with columns " + columnNames + " and types " + columnTypes);
        return databricksSchema.createTable("tab", columnNames, columnTypes) //
                .bulkInsert(getDatabricksRowValues());
    }

    private Stream<List<Object>> getDatabricksRowValues() {
        final int rowCount = this.columnTests.stream() //
                .map(ColumnTypeTest::databricksValues) //
                .mapToInt(List::size).max().orElseThrow();
        return IntStream.range(0, rowCount).mapToObj(this::collectRow);
    }

    private List<Object> collectRow(final int rowIndex) {
        return Stream.concat(Stream.of(rowIndex), this.columnTests.stream() //
                .map(ColumnTypeTest::databricksValues) //
                .map(values -> values.size() > rowIndex ? values.get(rowIndex) : null)) //
                .toList();
    }

    private static record ExpectedExasolType(String exasolType, Long maxSize, Long precision, Long scale) {
    }

    private static record ColumnTypeTest(String columnName, String databricksType, ExpectedExasolType expectedType,
            List<Object> databricksValues, List<Object> expectedValues) {

        DynamicNode createTest(final Table databricksTable, final ExaColumn actualType, final TableData actualData) {
            return DynamicContainer.dynamicContainer("Databricks Type " + databricksType,
                    Stream.concat(testExasolColumnType(databricksTable, actualType), testExpectedValues(actualData)));
        }

        private Stream<DynamicNode> testExasolColumnType(final Table databricksTable, final ExaColumn actual) {
            if (expectedType == null) {
                return Stream.empty();
            }
            final ExaColumn expected = new ExaColumn(databricksTable.getName().toUpperCase(), columnName.toUpperCase(),
                    expectedType.exasolType, expectedType.maxSize, expectedType.precision, expectedType.scale);
            return Stream.of(DynamicTest.dynamicTest("Exasol type " + expected.type(),
                    () -> assertThat(actual, AutoMatcher.equalTo(expected))));
        }

        private Stream<DynamicNode> testExpectedValues(final TableData actualData) {
            if (expectedValues.isEmpty()) {
                return Stream.empty();
            }
            final List<Object> actualColumnData = actualData.getColumnData(columnName.toUpperCase(),
                    expectedValues.size());
            final List<DynamicNode> tests = new ArrayList<>(expectedValues.size());
            for (int i = 0; i < expectedValues.size(); i++) {
                final Object expectedValue = expectedValues.get(i);
                final Object actualValue = actualColumnData.get(i);
                tests.add(DynamicTest.dynamicTest("Value " + expectedValue, () -> {
                    if (expectedValue != null) {
                        assertAll(() -> assertThat("value type", actualValue, instanceOf(expectedValue.getClass())),
                                () -> assertThat("value", actualValue, equalTo(expectedValue)));
                    } else {
                        assertThat(actualValue, nullValue());
                    }
                }));
            }
            return tests.stream();
        }
    }
}
