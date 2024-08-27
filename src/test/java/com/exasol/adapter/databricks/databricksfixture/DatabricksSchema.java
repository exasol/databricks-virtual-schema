package com.exasol.adapter.databricks.databricksfixture;

import com.databricks.sdk.service.catalog.SchemaInfo;
import com.exasol.db.Identifier;
import com.exasol.dbbuilder.dialects.*;

public class DatabricksSchema extends AbstractSchema {

    private final DatabricksObjectWriter writer;
    private final SchemaInfo schemaInfo;
    private final DatabricksCatalog parent;

    DatabricksSchema(final Identifier name, final SchemaInfo schemaInfo, final DatabricksCatalog parent,
            final DatabricksObjectWriter writer) {
        super(name);
        this.schemaInfo = schemaInfo;
        this.parent = parent;
        this.writer = writer;
    }

    @Override
    protected DatabaseObjectWriter getWriter() {
        return writer;
    }

    @Override
    protected Identifier getIdentifier(final String name) {
        return DatabricksIdentifier.of(name);
    }

    @Override
    public void markDeleted() {
        super.markDeleted();
    }

    @Override
    public boolean hasParent() {
        return true;
    }

    @Override
    public DatabaseObject getParent() {
        return parent;
    }
}
