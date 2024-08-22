package com.exasol.adapter.databricks.databricksfixture;

import java.util.Objects;

import com.exasol.db.Identifier;

class DatabricksIdentifier implements Identifier {
    private final String id;

    private DatabricksIdentifier(final String id) {
        this.id = id;
    }

    public static DatabricksIdentifier of(final String id) {
        return new DatabricksIdentifier(id);
    }

    @Override
    public String quote() {
        return "`" + this.id.replace("`", "``") + "`";
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DatabricksIdentifier other = (DatabricksIdentifier) obj;
        return Objects.equals(id, other.id);
    }

}
