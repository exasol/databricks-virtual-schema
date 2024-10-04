package com.exasol.adapter.databricks.fixture.exasol;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.exasol.dbbuilder.dialects.Table;

public class MetadataDao {
    private static final Logger LOG = Logger.getLogger(MetadataDao.class.getName());
    private final Connection connection;

    MetadataDao(final Connection connection) {
        this.connection = connection;
    }

    interface RowMapper<T> {
        T map(ResultSet resultSet) throws SQLException;
    }

    private <T> List<T> queryList(final String query, final List<Object> parameters, final RowMapper<T> rowMapper) {
        LOG.info(() -> "Executing query '" + query + "' with " + parameters.size() + " parameters...");
        return query(query, parameters, resultSet -> {
            try {
                final List<T> result = new ArrayList<>();
                while (resultSet.next()) {
                    result.add(rowMapper.map(resultSet));
                }
                return result;
            } catch (final SQLException exception) {
                throw new IllegalStateException("Failed to read result set: " + exception.getMessage(), exception);
            }
        });
    }

    <T> T query(final String query, final List<Object> parameters, final ResultSetProcessor<T> resultSetProcessor) {
        if (parameters.isEmpty()) {
            return queryWithoutParameters(query, resultSetProcessor);
        } else {
            return queryWithParameters(query, parameters, resultSetProcessor);
        }
    }

    @FunctionalInterface
    interface ResultSetProcessor<T> {
        T process(ResultSet resultSet) throws SQLException;
    }

    private <T> T queryWithParameters(final String query, final List<Object> parameters,
            final ResultSetProcessor<T> resultSetProcessor) {
        LOG.info(() -> "Executing query '" + query + "' with " + parameters.size() + " parameters...");
        try (final PreparedStatement statement = this.prepareStatement(query, parameters);
                ResultSet resultSet = getResultSet(statement)) {
            return resultSetProcessor.process(resultSet);
        } catch (final SQLException exception) {
            throw new IllegalStateException(String.format("Unable to execute query '%s' with parameters %s: %s", query,
                    parameters, exception.getMessage()), exception);
        }
    }

    private <T> T queryWithoutParameters(final String query, final ResultSetProcessor<T> resultSetProcessor) {
        try (final Statement statement = this.connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            return resultSetProcessor.process(resultSet);
        } catch (final SQLException exception) {
            throw new IllegalStateException(
                    String.format("Unable to execute query '%s': %s", query, exception.getMessage()), exception);
        }
    }

    private ResultSet getResultSet(final PreparedStatement statement) throws SQLException {
        if (statement.execute()) {
            return statement.getResultSet();
        } else {
            LOG.fine("Got update count " + statement.getUpdateCount());
            if (statement.getMoreResults()) {
                return statement.getResultSet();
            } else {
                throw new IllegalStateException("No further result sets");
            }
        }
    }

    private PreparedStatement prepareStatement(final String query, final List<Object> parameters) throws SQLException {
        final PreparedStatement statement = this.connection.prepareStatement(query);
        for (int i = 0; i < parameters.size(); i++) {
            statement.setObject(i + 1, parameters.get(i));
        }
        return statement;
    }

    public List<ExaColumn> getVirtualColumns(final ExasolVirtualSchema virtualSchema, final Table databricksTable) {
        return queryList("""
                select COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE
                from SYS.EXA_ALL_COLUMNS
                where COLUMN_SCHEMA = ? and COLUMN_TABLE = ?
                order by COLUMN_TABLE, COLUMN_ORDINAL_POSITION
                """, List.of(virtualSchema.getName(), databricksTable.getName().toUpperCase()),
                ExaColumn::fromResultSet);
    }

    public List<ExaColumn> getVirtualColumns(final ExasolVirtualSchema virtualSchema) {
        return queryList("""
                select COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE
                from SYS.EXA_ALL_COLUMNS
                where COLUMN_SCHEMA = ?
                order by COLUMN_TABLE, COLUMN_ORDINAL_POSITION
                """, List.of(virtualSchema.getName()), ExaColumn::fromResultSet);
    }

    public String getTableAdapterNotes(final ExasolVirtualSchema virtualSchema, final Table databricksTable) {
        final List<String> adapterNotes = queryList("""
                select "ADAPTER_NOTES"
                from "EXA_ALL_VIRTUAL_TABLES"
                where TABLE_SCHEMA=? and TABLE_NAME=?
                """, List.of(virtualSchema.getName(), databricksTable.getName().toUpperCase()),
                rs -> rs.getString("ADAPTER_NOTES"));
        assertThat("Expected exactly one entry for virtual table", adapterNotes, hasSize(1));
        return adapterNotes.get(0);
    }

    public String getColumnAdapterNotes(final ExasolVirtualSchema virtualSchema, final Table databricksTable,
            final String columnName) {
        final List<String> adapterNotes = queryList("""
                select "ADAPTER_NOTES"
                from "EXA_ALL_VIRTUAL_COLUMNS"
                where COLUMN_SCHEMA=? and COLUMN_TABLE=? and COLUMN_NAME=?
                """,
                List.of(virtualSchema.getName(), databricksTable.getName().toUpperCase(), columnName.toUpperCase()),
                rs -> rs.getString("ADAPTER_NOTES"));
        assertThat("Expected exactly one entry for virtual column", adapterNotes, hasSize(1));
        return adapterNotes.get(0);
    }

    public static record ExaColumn(String table, String columnName, String type, Long maxSize, Long numPrecision,
            Long numScale) {
        static ExaColumn fromResultSet(final ResultSet resultSet) throws SQLException {
            return new ExaColumn(resultSet.getString("COLUMN_TABLE"), resultSet.getString("COLUMN_NAME"),
                    resultSet.getString("COLUMN_TYPE"), (Long) resultSet.getObject("COLUMN_MAXSIZE"),
                    (Long) resultSet.getObject("COLUMN_NUM_PREC"), (Long) resultSet.getObject("COLUMN_NUM_SCALE"));
        }
    }

    public List<PushdownSql> explainVirtual(final String query) {
        return queryList("EXPLAIN VIRTUAL " + query, emptyList(), PushdownSql::fromResultSet);
    }

    public static record PushdownSql(int id, String sql, String json) {
        static PushdownSql fromResultSet(final ResultSet resultSet) throws SQLException {
            return new PushdownSql(resultSet.getInt("PUSHDOWN_ID"), resultSet.getString("PUSHDOWN_SQL"),
                    resultSet.getString("PUSHDOWN_JSON"));
        }

        public String extractSelectQuery() {
            final Pattern pattern = Pattern.compile("^IMPORT INTO .* FROM JDBC AT .* STATEMENT '(.*)'$");
            final java.util.regex.Matcher matcher = pattern.matcher(sql());
            assertTrue(matcher.matches(), "IMPORT statement '" + sql() + "' does not match regex '" + pattern + "'");
            return matcher.group(1).replace("''", "'");
        }
    }

    public TableData getTableData(final String query) {
        return query(query, emptyList(), TableData::create);
    }

    public static class TableData {
        private final List<TableRow> rows;
        private final List<String> columnNames;

        private TableData(final List<TableRow> rows, final List<String> columnNames) {
            this.rows = rows;
            this.columnNames = columnNames;
        }

        static TableData create(final ResultSet resultSet) throws SQLException {
            final int columnCount = resultSet.getMetaData().getColumnCount();
            final List<TableRow> columnValues = new ArrayList<>(columnCount);
            final List<String> columnNames = getColumnNames(resultSet);
            while (resultSet.next()) {
                columnValues.add(TableRow.create(resultSet, columnNames));
            }
            return new TableData(columnValues, columnNames);
        }

        private static List<String> getColumnNames(final ResultSet resultSet) throws SQLException {
            final int columnCount = resultSet.getMetaData().getColumnCount();
            final List<String> columnNames = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(resultSet.getMetaData().getColumnName(i));
            }
            assertThat(columnNames, hasSize(columnCount));
            return columnNames;
        }

        public List<Object> getColumnData(final String columnName, final int maxRowCount) {
            return this.rows.stream() //
                    .map(row -> row.getColumnValue(columnName)) //
                    .limit(maxRowCount) //
                    .toList();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            for (final String column : columnNames) {
                sb.append(column).append(" ");
            }
            sb.append("\n");
            for (final TableRow row : rows) {
                sb.append(row.toString()).append("\n");
            }
            return sb.toString();
        }
    }

    public static class TableRow {
        private final List<String> columnNames;
        private final Map<String, Object> valuesByColumnName;

        private TableRow(final Map<String, Object> valuesByColumnName, final List<String> columnNames) {
            this.valuesByColumnName = valuesByColumnName;
            this.columnNames = columnNames;
        }

        public static TableRow create(final ResultSet resultSet, final List<String> columnNames) throws SQLException {
            final int columnCount = resultSet.getMetaData().getColumnCount();
            final Map<String, Object> values = new HashMap<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                final String columnName = columnNames.get(i - 1);
                final Object columnValue = getColumnValue(resultSet, i);
                values.put(columnName, columnValue);
            }
            return new TableRow(values, columnNames);
        }

        private static Object getColumnValue(final ResultSet resultSet, final int columnIndex) throws SQLException {
            final JDBCType columnType = JDBCType.valueOf(resultSet.getMetaData().getColumnType(columnIndex));
            if (columnType == JDBCType.TIMESTAMP) {
                final Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                return resultSet.getTimestamp(columnIndex, utcCalendar);
            }
            return resultSet.getObject(columnIndex);
        }

        public Object getColumnValue(final String columnName) {
            return valuesByColumnName.get(columnName);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            for (final String column : columnNames) {
                sb.append(getColumnValue(column)).append(" ");
            }
            return sb.toString();
        }
    }
}
