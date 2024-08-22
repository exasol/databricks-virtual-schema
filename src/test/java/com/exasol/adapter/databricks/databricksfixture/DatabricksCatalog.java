package com.exasol.adapter.databricks.databricksfixture;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.databricks.sdk.service.catalog.CatalogInfo;
import com.exasol.db.Identifier;
import com.exasol.dbbuilder.dialects.AbstractDatabaseObject;
import com.exasol.dbbuilder.dialects.DatabaseObject;

public class DatabricksCatalog extends AbstractDatabaseObject {
    private static final Logger LOG = Logger.getLogger(DatabricksCatalog.class.getName());
    private final DatabricksObjectWriter writer;
    private final List<DatabricksSchema> schemas = new ArrayList<>();

    DatabricksCatalog(final Identifier name, final CatalogInfo catalogInfo, final DatabricksObjectWriter writer) {
        super(name, true);
        this.writer = writer;
    }

    public DatabricksSchema createSchema(final String name) {
        final DatabricksSchema schema = writer.createSchema(this, name);
        this.schemas.add(schema);
        return schema;
    }

    @Override
    public String getType() {
        return "CATALOG";
    }

    @Override
    public boolean hasParent() {
        return false;
    }

    @Override
    public DatabaseObject getParent() {
        throw new UnsupportedOperationException("Catalog has no parent");
    }

    @Override
    protected void dropInternally() {
        LOG.fine(() -> "Dropping catalog " + this.getFullyQualifiedName() + " with " + schemas.size() + " schemas");
        writer.drop(this);
        schemas.forEach(schema -> schema.markDeleted());
        this.schemas.clear();
    }
}
