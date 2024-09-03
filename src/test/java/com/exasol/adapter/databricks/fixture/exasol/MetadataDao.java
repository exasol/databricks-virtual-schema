package com.exasol.adapter.databricks.fixture.exasol;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

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

    <T> T query(final String query, final List<Object> parameters, final Function<ResultSet, T> resultSetProcessor) {
        if (parameters.isEmpty()) {
            return queryWithoutParameters(query, resultSetProcessor);
        } else {
            return queryWithParameters(query, parameters, resultSetProcessor);
        }
    }

    private <T> T queryWithParameters(final String query, final List<Object> parameters,
            final Function<ResultSet, T> resultSetProcessor) {
        LOG.info(() -> "Executing query '" + query + "' with " + parameters.size() + " parameters...");
        try (final PreparedStatement statement = this.prepareStatement(query, parameters);
                ResultSet resultSet = getResultSet(statement)) {
            return resultSetProcessor.apply(resultSet);
        } catch (final SQLException exception) {
            throw new IllegalStateException("Unable to execute query: '" + query + "'", exception);
        }
    }

    private <T> T queryWithoutParameters(final String query, final Function<ResultSet, T> resultSetProcessor) {
        LOG.info(() -> "Executing query '" + query + "'...");
        try (final Statement statement = this.connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            return resultSetProcessor.apply(resultSet);
        } catch (final SQLException exception) {
            throw new IllegalStateException("Unable to execute query: '" + query + "'", exception);
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

    public List<ExaColumn> getVirtualColumns(final VirtualSchema virtualSchema, final Table databricksTable) {
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
