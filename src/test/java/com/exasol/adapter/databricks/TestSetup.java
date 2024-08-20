package com.exasol.adapter.databricks;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.exasol.containers.ExasolContainer;
import com.exasol.dbbuilder.dialects.exasol.*;
import com.exasol.mavenprojectversiongetter.MavenProjectVersionGetter;

class TestSetup implements AutoCloseable {

    private static Logger LOG = Logger.getLogger(TestSetup.class.getName());
    private static final String VERSION = MavenProjectVersionGetter.getCurrentProjectVersion();
    private static final Path ADAPTER_PATH = Path.of("target/databricks-virtual-schema-dist-" + VERSION + ".lua");

    private static final String DEFAULT_EXASOL_VERSION = "8.29.1";
    private final ExasolContainer<? extends ExasolContainer<?>> exasol;
    private final Connection connection;
    private final ExasolObjectFactory objectFactory;
    private AdapterScript adapterScript;

    private TestSetup(final ExasolContainer<? extends ExasolContainer<?>> exasol, final Connection connection,
            final ExasolObjectFactory objectFactory) {
        this.exasol = exasol;
        this.connection = connection;
        this.objectFactory = objectFactory;
    }

    public static TestSetup start() {
        final ExasolContainer<? extends ExasolContainer<?>> exasol = new ExasolContainer<>(DEFAULT_EXASOL_VERSION) //
                .withReuse(true);
        exasol.start();
        exasol.purgeDatabase();
        final Connection connection = exasol.createConnection();
        final ExasolObjectFactory objectFactory = new ExasolObjectFactory(connection);

        return new TestSetup(exasol, connection, objectFactory);
    }

    private AdapterScript createAdapterScript() {
        final ExasolSchema adapterSchema = objectFactory.createSchema("ADAPTER_SCRIPT_SCHEMA");
        return adapterSchema.createAdapterScript("DATABRICKS_VS_ADAPTER", AdapterScript.Language.LUA,
                readAdapterContent());
    }

    public VirtualSchema createVirtualSchema() {
        if (this.adapterScript == null) {
            this.adapterScript = createAdapterScript();
        }
        final Map<String, String> properties = new HashMap<>();
        return objectFactory.createVirtualSchemaBuilder("DATABRICKS_VS_" + System.currentTimeMillis())
                .adapterScript(this.adapterScript) //
                .properties(properties) //
                .build();
    }

    private static String readAdapterContent() {
        try {
            return Files.readString(ADAPTER_PATH);
        } catch (final IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (final SQLException exception) {
            LOG.log(Level.WARNING, "Failed to close connection", exception);
        }
        this.exasol.close();
    }
}
