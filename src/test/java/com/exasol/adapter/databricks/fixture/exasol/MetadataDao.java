package com.exasol.adapter.databricks.fixture.exasol;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

public class MetadataDao {

    private final Connection connection;

    MetadataDao(final Connection connection) {
        this.connection = connection;
    }

    interface RowMapper<T> {
        T map(ResultSet resultSet) throws SQLException;
    }

    private <T> List<T> queryList(final String query, final List<Object> parameters, final RowMapper<T> rowMapper) {
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

    <T> T query(final String query, final List<Object> parameters, final Function<ResultSet, T> resultSetProcessor) {
        try (final PreparedStatement statement = this.prepareStatement(query, parameters);
                final ResultSet resultSet = statement.executeQuery()) {
            return resultSetProcessor.apply(resultSet);
        } catch (final SQLException exception) {
            throw new IllegalStateException("Unable to execute query: '" + query + "'", exception);
        }
    }

    private PreparedStatement prepareStatement(final String query, final List<Object> parameters) throws SQLException {
        final PreparedStatement statement = this.connection.prepareStatement(query);
        for (int i = 0; i < parameters.size(); i++) {
            statement.setObject(i + 1, parameters.get(i));
        }
        return statement;
    }

    public List<ExaColumn> getVirtualColumns(final VirtualSchema virtualSchema, final Table databricksTable) {
        // COLUMN_IS_NULLABLE and COLUMN_IDENTITY is are not filled for virtual schemas, so we cannot check them
        return queryList("""
                select COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE
                from SYS.EXA_ALL_COLUMNS
                where COLUMN_SCHEMA = ? and COLUMN_TABLE = ?
                order by COLUMN_TABLE, COLUMN_ORDINAL_POSITION
                """, List.of(virtualSchema.getName(), databricksTable.getName()), ExaColumn::fromResultSet);
    }

    public static record ExaColumn(String columnName, String type, Long maxSize, Long numPrecision, Long numScale) {
        static ExaColumn fromResultSet(final ResultSet resultSet) throws SQLException {
            return new ExaColumn(resultSet.getString(1), resultSet.getString(2), (Long) resultSet.getObject(3),
                    (Long) resultSet.getObject(4), (Long) resultSet.getObject(5));
        }
    }
}
