package com.exasol.adapter.databricks.fixture.exasol;

import static java.util.Collections.emptyMap;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture;
import com.exasol.adapter.databricks.databricksfixture.DatabricksFixture.AuthMode;
import com.exasol.adapter.databricks.databricksfixture.DatabricksSchema;
import com.exasol.adapter.databricks.fixture.CleanupActions;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.dbbuilder.dialects.exasol.*;
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

    private static final String DEFAULT_EXASOL_VERSION = "8.31.0";
    private final ExasolContainer<? extends ExasolContainer<?>> exasol;
    private final Connection connection;
    private final ExasolObjectFactory objectFactory;
    private final UdfLogCapturer udfLogCapturer;
    private final DatabricksFixture databricksFixture;
    private final CleanupActions cleanupAfterTest = new CleanupActions();
    private final CleanupActions cleanupAfterAll = new CleanupActions();
    private AdapterScript adapterScript;
    private final Map<AuthMode, ConnectionDefinition> connectionDefinition = new HashMap<>();

    private ExasolFixture(final ExasolContainer<? extends ExasolContainer<?>> exasol, final Connection connection,
            final ExasolObjectFactory objectFactory, final UdfLogCapturer udfLogCapturer,
            final DatabricksFixture databricksFixture) {
        this.exasol = exasol;
        this.connection = connection;
        this.objectFactory = objectFactory;
        this.udfLogCapturer = udfLogCapturer;
        this.databricksFixture = databricksFixture;
    }

    public static ExasolFixture start(final DatabricksFixture databricksFixture) {
        @SuppressWarnings("resource") // Resource will be closed in close() method
        final ExasolContainer<? extends ExasolContainer<?>> exasol = new ExasolContainer<>(DEFAULT_EXASOL_VERSION) //
                .withReuse(true);
        exasol.start();
        installJdbcDriver(exasol);
        final Connection connection = exasol.createConnection();
        final ExasolObjectFactory objectFactory = new ExasolObjectFactory(connection,
                ExasolObjectConfiguration.builder().build());
        final UdfLogCapturer udfLogCapturer = UdfLogCapturer.start();
        return new ExasolFixture(exasol, connection, objectFactory, udfLogCapturer, databricksFixture);
    }

    /**
     * Install the JDBC driver and register it for ExaLoader.
     * <p>
     * This intentionally does not use {@link ExasolContainer#getDriverManager()} /
     * {@link com.exasol.drivers.ExasolDriverManager} because we want to reproduce the same configuration as recommended
     * in the user guide:
     * <ul>
     * <li>install driver in path {@code drivers/jdbc/databricks} instead of {@code drivers/jdbc}</li>
     * <li>omit entries {@code JAR} and {@code DRIVERMAIN} in {@code settings.cfg}</li>
     * </ul>
     * 
     * @param exasol Exasol container
     */
    private static void installJdbcDriver(final ExasolContainer<? extends ExasolContainer<?>> exasol) {
        final String driverBasePath = "drivers/jdbc/databricks/";
        final String jdbcDriverFileName = JDBC_DRIVER_PATH.getFileName().toString();
        final String settingsCfgContent = """
                DRIVERNAME=DATABRICKS
                PREFIX=jdbc:databricks:
                NOSECURITY=YES
                FETCHSIZE=100000
                INSERTSIZE=-1

                """; // Note the trailing newline!
        try {
            exasol.getDefaultBucket().uploadFile(JDBC_DRIVER_PATH, driverBasePath + jdbcDriverFileName);
            exasol.getDefaultBucket().uploadStringContent(settingsCfgContent, driverBasePath + "settings.cfg");
        } catch (FileNotFoundException | BucketAccessException | TimeoutException exception) {
            throw new IllegalStateException("Failed to install JDBC driver", exception);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
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

    public ExasolVirtualSchema createVirtualSchema(final DatabricksSchema databricksSchema) {
        return createVirtualSchema(databricksSchema, emptyMap());
    }

    public ExasolVirtualSchema createVirtualSchema(final DatabricksSchema databricksSchema,
            final Map<String, String> properties) {
        return createVirtualSchema(databricksSchema.getParent().getName(), properties, AuthMode.TOKEN);
    }

    public ExasolVirtualSchema createVirtualSchema(final DatabricksSchema databricksSchema,
            final Map<String, String> properties, final AuthMode authMode) {
        return createVirtualSchema(databricksSchema.getParent().getName(), databricksSchema.getName(), properties,
                authMode);
    }

    public ExasolVirtualSchema createVirtualSchema(final String databricksCatalog, final String databricksSchema,
            final Map<String, String> additionalProperties, final AuthMode authMode) {
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(additionalProperties);
        if (databricksCatalog != null) {
            properties.put("CATALOG_NAME", databricksCatalog);
        }
        if (databricksSchema != null) {
            properties.put("SCHEMA_NAME", databricksSchema);
        }
        return createVirtualSchema(properties, authMode);
    }

    private ExasolVirtualSchema createVirtualSchema(final Map<String, String> properties, final AuthMode authMode) {
        return createVirtualSchema("DATABRICKS_VS", properties, authMode);
    }

    private ExasolVirtualSchema createVirtualSchema(final String vsName, final Map<String, String> additionalProperties,
            final AuthMode authMode) {
        LOG.fine("Creating virtual schema '" + vsName + "'' with properties " + additionalProperties);
        final Map<String, String> properties = createVirtualSchemaProperties(getConnectionDefinition(authMode));
        properties.putAll(additionalProperties);
        final VirtualSchema virtualSchema = objectFactory.createVirtualSchemaBuilder(vsName) //
                .adapterScript(getAdapterScript()) //
                .properties(properties) //
                .build();
        this.cleanupAfterTest.add("Drop virtual schema " + virtualSchema.getName(), () -> virtualSchema.drop());
        return new ExasolVirtualSchema(this, virtualSchema);
    }

    private AdapterScript getAdapterScript() {
        if (this.adapterScript == null) {
            this.adapterScript = createAdapterScript();
        }
        return this.adapterScript;
    }

    public ConnectionDefinition getConnectionDefinition(final AuthMode authMode) {
        if (!this.connectionDefinition.containsKey(authMode)) {
            final ConnectionDefinition connectionDef = objectFactory.createConnectionDefinition(
                    "DATABRICKS_CONNECTION_" + authMode.toString().toUpperCase(),
                    databricksFixture.getJdbcUrl(authMode), databricksFixture.getJdbcUsername(authMode),
                    databricksFixture.getJdbcPassword(authMode));
            this.cleanupAfterAll.add("Drop connection " + connectionDef.getName(), () -> connectionDef.drop());
            this.connectionDefinition.put(authMode, connectionDef);
        }
        return this.connectionDefinition.get(authMode);
    }

    private Map<String, String> createVirtualSchemaProperties(final ConnectionDefinition connectionDefinition) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_NAME", connectionDefinition.getName());
        properties.put("DEBUG_ADDRESS", this.udfLogCapturer.getServerHost() + ":" + this.udfLogCapturer.getPort());
        properties.put("LOG_LEVEL", "TRACE");
        return properties;
    }

    public void executeStatement(final String sql) {
        try (Statement stmt = this.connection.createStatement()) {
            stmt.executeUpdate(sql);
        } catch (final SQLException exception) {
            throw new IllegalStateException("Failed to execute statement '" + sql + "': " + exception.getMessage(),
                    exception);
        }
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
        this.cleanupAfterTest.cleanup();
    }

    public void cleanupAfterAllTest() {
        this.cleanupAfterAll.cleanup();
    }

    @Override
    public void close() {
        this.cleanupAfterTest();
        this.udfLogCapturer.close();
        try {
            this.connection.close();
        } catch (final SQLException exception) {
            LOG.log(Level.WARNING, "Failed to close connection", exception);
        }
        this.exasol.close();
    }
}
