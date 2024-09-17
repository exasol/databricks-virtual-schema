package com.exasol.adapter.databricks.fixture.exasol;

import com.exasol.db.ExasolIdentifier;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.VirtualSchema;

/**
 * This class wraps an Exasol {@link VirtualSchema} to add method {@link #qualifyTableName(Table)}.
 */
public class ExasolVirtualSchema {

    private final VirtualSchema virtualSchema;

    ExasolVirtualSchema(final VirtualSchema virtualSchema) {
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
        return this.virtualSchema.getFullyQualifiedName() + "." + ExasolIdentifier.of(table.getName()).quote();
    }
}
