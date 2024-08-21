package com.exasol.adapter.databricks;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.exasol.containers.ExasolContainer;
import com.exasol.dbbuilder.dialects.exasol.*;
import com.exasol.mavenprojectversiongetter.MavenProjectVersionGetter;

class TestSetup implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(TestSetup.class.getName());
    private static final String EXASOL_LUA_MODULE_LOADER_WORKAROUND = "table.insert(" //
            + "package.searchers" //
            + ",\n" //
            + "    function (module_name)\n" //
            + "        local loader = package.preload[module_name]\n" //
            + "        if(loader == nil) then\n" //
            + "            error(\"Module \" .. module_name .. \" not found in package.preload.\")\n" //
            + "        else\n" //
            + "            return loader\n" //
            + "        end\n" //
            + "    end\n" //
            + ")\n\n";
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
        final ExasolObjectFactory objectFactory = new ExasolObjectFactory(connection,
                ExasolObjectConfiguration.builder().build());
        return new TestSetup(exasol, connection, objectFactory);
    }

    public void buildAdapter() {
        runProcess(List.of("luarocks", "make", "--local"));
    }

    private static void runProcess(final List<String> command) {
        final ProcessBuilder processBuilder = new ProcessBuilder(command).redirectErrorStream(true);
        LOG.info(() -> "Starting process " + command);
        try {
            final StringBuilder output = new StringBuilder();
            final Process process = processBuilder.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    LOG.info("command >" + line);
                }
            }
            final int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException(
                        "Command " + command + " failed with exit code " + exitCode + ", output: '" + output + "'");
            }
        } catch (final IOException exception) {
            throw new UncheckedIOException("Failed to run command " + command, exception);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    public VirtualSchema createVirtualSchema() {
        if (this.adapterScript == null) {
            this.adapterScript = createAdapterScript();
        }
        final Map<String, String> properties = new HashMap<>();
        return objectFactory.createVirtualSchemaBuilder("DATABRICKS_VS") //
                .adapterScript(this.adapterScript) //
                .properties(properties) //
                .build();
    }

    public DbAssertions assertions() {
        return new DbAssertions(this.connection);
    }

    private AdapterScript createAdapterScript() {
        final ExasolSchema adapterSchema = objectFactory.createSchema("ADAPTER_SCRIPT_SCHEMA");
        return adapterSchema.createAdapterScript("DATABRICKS_VS_ADAPTER", AdapterScript.Language.LUA,
                EXASOL_LUA_MODULE_LOADER_WORKAROUND + readAdapterContent());
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
