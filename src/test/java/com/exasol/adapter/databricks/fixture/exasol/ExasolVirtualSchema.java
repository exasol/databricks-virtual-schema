package com.exasol.adapter.databricks.fixture.exasol;

import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.Map;

import com.exasol.db.ExasolIdentifier;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

/**
 * This class wraps an Exasol {@link VirtualSchema} to add method {@link #qualifyTableName(Table)}.
 */
public class ExasolVirtualSchema {

    private final ExasolFixture exasolFixture;
    private final VirtualSchema virtualSchema;

    ExasolVirtualSchema(final ExasolFixture exasolFixture, final VirtualSchema virtualSchema) {
        this.exasolFixture = exasolFixture;
        this.virtualSchema = virtualSchema;
    }

    public String getName() {
        return virtualSchema.getName();
    }

    /**
     * Build fully qualified name of the given table in the Exasol virtual schema.
     * 
     * @param table remote table, e.g. Databricks
     * @return fully qualified table name in the virtual schema
     */
    public String qualifyTableName(final Table table) {
        return this.virtualSchema.getFullyQualifiedName() + "."
                + ExasolIdentifier.of(table.getName().toUpperCase()).quote();
    }

    public void setProperties(final Map<String, String> properties) {
        exasolFixture.executeStatement(String.format("ALTER VIRTUAL SCHEMA %s SET %s",
                virtualSchema.getFullyQualifiedName(), formatProperties(properties)));
    }

    public void refresh() {
        exasolFixture.executeStatement(
                String.format("ALTER VIRTUAL SCHEMA %s REFRESH", virtualSchema.getFullyQualifiedName()));
    }

    public void refresh(final List<String> tables) {
        exasolFixture.executeStatement(String.format("ALTER VIRTUAL SCHEMA %s REFRESH TABLES %s",
                virtualSchema.getFullyQualifiedName(), tables.stream().collect(joining(" "))));
    }

    private String formatProperties(final Map<String, String> properties) {
        return properties.entrySet().stream() //
                .map(p -> p.getKey() + "='" + p.getValue() + "'") //
                .collect(joining(" "));
    }
}
