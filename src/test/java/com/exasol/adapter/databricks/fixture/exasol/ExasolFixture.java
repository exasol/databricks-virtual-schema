package com.exasol.adapter.databricks.fixture.exasol;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture;
import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.TestConfig;
import com.exasol.containers.ExasolContainer;
import com.exasol.dbbuilder.dialects.exasol.*;
import com.exasol.drivers.JdbcDriver;
import com.exasol.mavenprojectversiongetter.MavenProjectVersionGetter;

public class ExasolFixture implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(ExasolFixture.class.getName());
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
    private static final Path TARGET_DIR = Path.of("target").toAbsolutePath();
    private static final Path ADAPTER_PATH = TARGET_DIR.resolve("databricks-virtual-schema-dist-" + VERSION + ".lua");
    private static final Path JDBC_DRIVER_PATH = TARGET_DIR.resolve("databricks-jdbc-driver/databricks-jdbc.jar");

    private static final String DEFAULT_EXASOL_VERSION = "8.29.1";
    private final ExasolContainer<? extends ExasolContainer<?>> exasol;
    private final Connection connection;
    private final ExasolObjectFactory objectFactory;
    private final UdfLogCapturer udfLogCapturer;
    private final DatabricksFixture databricksFixture;
    private final List<Runnable> cleanupTasks = new ArrayList<>();
    private AdapterScript adapterScript;
    private ConnectionDefinition connectionDefinition;

    private ExasolFixture(final ExasolContainer<? extends ExasolContainer<?>> exasol, final Connection connection,
            final ExasolObjectFactory objectFactory, final UdfLogCapturer udfLogCapturer,
            final DatabricksFixture databricksFixture) {
        this.exasol = exasol;
        this.connection = connection;
        this.objectFactory = objectFactory;
        this.udfLogCapturer = udfLogCapturer;
        this.databricksFixture = databricksFixture;
    }

    public static ExasolFixture start(final TestConfig config, final DatabricksFixture databricksFixture) {
        final ExasolContainer<? extends ExasolContainer<?>> exasol = new ExasolContainer<>(DEFAULT_EXASOL_VERSION) //
                .withReuse(true);
        exasol.start();
        exasol.getDriverManager()
                .install(JdbcDriver.builder("DATABRICKS").enableSecurityManager(false)
                        .mainClass("com.databricks.client.jdbc.Driver").prefix("jdbc:databricks")
                        .sourceFile(JDBC_DRIVER_PATH).build());
        final Connection connection = exasol.createConnection();
        final ExasolObjectFactory objectFactory = new ExasolObjectFactory(connection,
                ExasolObjectConfiguration.builder().build());
        final UdfLogCapturer udfLogCapturer = UdfLogCapturer.start();
        return new ExasolFixture(exasol, connection, objectFactory, udfLogCapturer, databricksFixture);
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
                    LOG.info("cmd>" + line);
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

    public VirtualSchema createVirtualSchema(final DatabricksSchema databricksSchema) {
        return createVirtualSchema(databricksSchema.getParent().getName(), databricksSchema.getName());
    }

    public VirtualSchema createVirtualSchema(final String databricksCatalog, final String databricksSchema) {
        final Map<String, String> properties = new HashMap<>();
        if (databricksCatalog != null) {
            properties.put("CATALOG_NAME", databricksCatalog);
        }
        if (databricksSchema != null) {
            properties.put("SCHEMA_NAME", databricksSchema);
        }
        return createVirtualSchema(properties);
    }

    private VirtualSchema createVirtualSchema(final Map<String, String> properties) {
        return createVirtualSchema("DATABRICKS_VS", properties);
    }

    private VirtualSchema createVirtualSchema(final String vsName, final Map<String, String> additionalProperties) {
        final Map<String, String> properties = createVirtualSchemaProperties(getConnectionDefinition());
        properties.putAll(additionalProperties);
        final VirtualSchema virtualSchema = objectFactory.createVirtualSchemaBuilder(vsName) //
                .adapterScript(getAdapterScript()) //
                .properties(properties) //
                .build();
        this.cleanupTasks.add(() -> virtualSchema.drop());
        return virtualSchema;
    }

    private AdapterScript getAdapterScript() {
        if (this.adapterScript == null) {
            this.adapterScript = createAdapterScript();
        }
        return this.adapterScript;
    }

    public ConnectionDefinition getConnectionDefinition() {
        if (this.connectionDefinition == null) {
            this.connectionDefinition = objectFactory.createConnectionDefinition("DATABRICKS_CONNECTION",
                    databricksFixture.getJdbcUrl(), databricksFixture.getJdbcUsername(),
                    databricksFixture.getJdbcPassword());
        }
        return this.connectionDefinition;
    }

    private Map<String, String> createVirtualSchemaProperties(final ConnectionDefinition connectionDefinition) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_NAME", connectionDefinition.getName());
        properties.put("DEBUG_ADDRESS", this.udfLogCapturer.getServerHost() + ":" + this.udfLogCapturer.getPort());
        properties.put("LOG_LEVEL", "TRACE");
        LOG.fine(() -> "Creating virtual schema with properties: " + properties);
        return properties;
    }

    public MetadataDao metadata() {
        return new MetadataDao(this.connection);
    }

    public DbAssertions assertions() {
        return new DbAssertions(this, metadata());
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

    public void cleanupAfterTest() {
        cleanupTasks.forEach(Runnable::run);
        cleanupTasks.clear();
    }

    @Override
    public void close() {
        this.udfLogCapturer.close();
        try {
            this.connection.close();
        } catch (final SQLException exception) {
            LOG.log(Level.WARNING, "Failed to close connection", exception);
        }
        this.exasol.close();
    }
}
