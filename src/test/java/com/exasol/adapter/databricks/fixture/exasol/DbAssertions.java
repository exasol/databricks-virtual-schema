package com.exasol.adapter.databricks.fixture.exasol;

import static com.exasol.matcher.ResultSetStructureMatcher.table;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.util.List;
import java.util.regex.Pattern;

import org.hamcrest.Matcher;

import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.dbbuilder.dialects.DatabaseObjectException;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

public class DbAssertions {

    private final MetadataDao metadataDao;
    private final ExasolFixture exasolFixture;

    DbAssertions(final ExasolFixture exasolFixture, final MetadataDao metadataDao) {
        this.exasolFixture = exasolFixture;
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

    private String extractLuaError(final String errorMessage) {
        final Pattern pattern = Pattern.compile("Lua Error .*\\|(.*?)\" caught in script.*", Pattern.DOTALL);
        final java.util.regex.Matcher matcher = pattern.matcher(errorMessage);
        if (matcher.find() && matcher.group(1) != null) {
            return matcher.group(1);
        } else {
            throw new IllegalArgumentException("No Lua error message found in '" + errorMessage + "'");
        }
    }

    public void assertVirtualSchemaFails(final DatabricksSchema databricksSchema,
            final Matcher<String> errorMessageMatcher) {
        final RuntimeException exception = assertThrows(DatabaseObjectException.class,
                () -> exasolFixture.createVirtualSchema(databricksSchema));
        assertThat(extractLuaError(exception.getCause().getMessage()), errorMessageMatcher);
    }
}
