package com.exasol.adapter.databricks.fixture.exasol;

import static com.exasol.matcher.ResultSetStructureMatcher.table;

import java.sql.ResultSet;
import java.util.List;

import org.hamcrest.Matcher;

import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

public class DbAssertions {

    private final MetadataDao metadataDao;

    DbAssertions(final MetadataDao metadataDao) {
        this.metadataDao = metadataDao;
    }

    public void query(final String query, final Matcher<ResultSet> matcher) {
        query(query, List.of(), matcher);
    }

    private void query(final String query, final List<Object> parameters, final Matcher<ResultSet> matcher) {
        metadataDao.query(query, parameters, resultSet -> {
            matcher.matches(resultSet);
            return null;
        });
    }

    public void virtualSchemaExists(final VirtualSchema virtualSchema) {
        query("""
                select SCHEMA_NAME, ADAPTER_SCRIPT_SCHEMA, ADAPTER_SCRIPT_NAME, ADAPTER_NOTES
                from EXA_ALL_VIRTUAL_SCHEMAS
                """, table().row(virtualSchema.getName(), "ADAPTER_SCRIPT_SCHEMA", "DATABRICKS_VS_ADAPTER", "notes")
                .matches());
    }
}
