package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.policy.InfoPolicy;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.ListRecordSet;
import com.nosqldriver.sql.SimpleWrapper;
import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.SneakyThrower;
import com.nosqldriver.util.ThrowingSupplier;
import com.nosqldriver.util.TypeGroup;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.Manifest;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.lang.String.format;
import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.JDBCType.OTHER;
import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BLOB;
import static java.sql.Types.CHAR;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedSet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class AerospikeDatabaseMetadata implements DatabaseMetaData, SimpleWrapper {
    private final static ConnectionParametersParser parser = new ConnectionParametersParser();
    private final String url;
    private final Properties clientInfo;

    private final Optional<Manifest> manifest;
    private final Connection connection;
    private final InfoPolicy infoPolicy;
    private final int discoverMetadataLines;
    private final FunctionManager functionManager;
    private static final String newLine = System.lineSeparator();
    private final String dbBuild;
    private final String dbEdition;
    private final List<String> catalogs;
    private final Map<String, Collection<String>> tables = new ConcurrentHashMap<>();
    private final Map<String, Collection<IndexInfo>> indices = new ConcurrentHashMap<>();


    public AerospikeDatabaseMetadata(String url, Properties info, IAerospikeClient client, Connection connection, AerospikePolicyProvider policyProvider, FunctionManager functionManager) {
        this.url = url;
        clientInfo = parser.clientInfo(url, info);
        this.connection = connection;
        infoPolicy = policyProvider.getInfoPolicy();
        discoverMetadataLines = policyProvider.getDriverPolicy().discoverMetadataLines;
        this.functionManager = functionManager;
        manifest = manifest();

        Collection<String> builds = synchronizedSet(new HashSet<>());
        Collection<String> editions = synchronizedSet(new HashSet<>());
        Collection<String> namespaces = synchronizedSet(new HashSet<>());
        Arrays.stream(client.getNodes()).parallel()
                .map(node -> Info.request(infoPolicy, node, "namespaces", "sets", "sindex-list:", "build", "edition"))
                .forEach(r -> {
                    builds.add(r.get("build"));
                    editions.add(r.get("edition"));
                    namespaces.addAll(Arrays.asList(getOrDefault(r, "namespaces", "").split(";")));
                    streamOfSubProperties(r, "sets").forEach(p -> tables.computeIfAbsent(p.getProperty("ns"), s -> new HashSet<>()).add(p.getProperty("set")));
                    streamOfSubProperties(r, "sindex-list:").forEach(p -> indices.computeIfAbsent(p.getProperty("ns"), s -> new HashSet<>())
                            .add(new IndexInfo(p.getProperty("ns"), p.getProperty("set"), p.getProperty("indexname"), p.getProperty("bin"), p.getProperty("type"))));
                });

        dbBuild = join("N/A", ", ", builds);
        dbEdition = join("Aerospike", ", ", editions);
        catalogs = namespaces.stream().filter(n -> !"".equals(n)).collect(Collectors.toList());
    }

    private Properties initProperties(String lines) {
        return iosafe(() -> {
            Properties properties = new Properties();
            properties.load(new StringReader(lines));
            return properties;
        });
    }

    private Stream<Properties> streamOfSubProperties(Map<String, String> map, String key) {
        return Optional.ofNullable(map.get(key)).map(s -> Arrays.stream(s.split(";")).map(ns -> initProperties(ns.replace(":", newLine)))).orElse(Stream.empty());
    }

    /**
     * Truly {@code getOrDefault()} - returns value from map only if it exists and is not {@code null}.
     * Otherwise returns default value.
     * @param map - the map
     * @param key - the key
     * @param defaultValue - the default value
     * @param <K> - the key type
     * @param <V> - the value type
     * @return not null value or default value
     */
    private <K, V> V getOrDefault(Map<K, V> map, K key, V defaultValue) {
        return Optional.ofNullable(map.getOrDefault(key, defaultValue)).orElse(defaultValue);
    }

    private String join(String defaultValue, String delimiter, Collection<String> elements) {
        return elements.isEmpty() ? defaultValue : String.join(delimiter, elements);
    }

    @Override
    public boolean allProceduresAreCallable() {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public String getUserName() {
        return clientInfo.getProperty("user");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    @Override
    public String getDatabaseProductName() {
        return dbEdition;
    }

    @Override
    public String getDatabaseProductVersion() {
        return dbBuild;
    }

    @Override
    public String getDriverName() {
        return AerospikeDriver.class.getName();
    }

    @Override
    public String getDriverVersion() {
        return fetchDriverVersion().orElse("N/A");
    }

    @Override
    public int getDriverMajorVersion() {
        return fetchDriverVersion().map(v -> Integer.parseInt(v.split("\\.")[0])).orElse(1);
    }

    @Override
    public int getDriverMinorVersion() {
        return fetchDriverVersion().map(v -> v.split("\\.")).map(a -> a.length > 1 ? Integer.parseInt(a[a.length -1]) : 0).orElse(0);
    }

    private Optional<String> fetchDriverVersion() {
        return manifest.map(m -> (String)m.getMainAttributes().get("Implementation-Version"));
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return getFunctions(Number.class) + "sum,sumsqs,avg,min,max,count";
    }

    @Override
    public String getStringFunctions() {
        return getFunctions(String.class);
    }

    private String getFunctions(Class<?> type) {
        return functionManager.getFunctionNames().stream()
                .filter(name -> Optional.ofNullable(functionManager.getFunction(name).getClass().getAnnotation(TypeGroup.class))
                    .map(g -> Arrays.asList(g.value()).contains(type)).orElse(false)).collect(Collectors.joining(","));
    }

    @Override
    public String getSystemFunctions() {
        return getFunctions(System.class);
    }

    @Override
    public String getTimeDateFunctions() {
        return getFunctions(Date.class);
    }

    @Override
    public String getSearchStringEscape() {
        return "\\"; //TODO: ?
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return false;
    }

    @Override
    public boolean supportsConvert() {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public String getSchemaTerm() {
        return "";
    }

    @Override
    public String getProcedureTerm() {
        return "lua script";
    }

    @Override
    public String getCatalogTerm() {
        return "namespace";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return false;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 14;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 14;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 1;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 32767;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 32767;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 256;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 14;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 14;
    }

    @Override
    public int getMaxRowSize() {
        return 8 * 1024 * 1024;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return false;
    }

    @Override
    public int getMaxStatementLength() {
        return 0;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 63;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 63;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return TRANSACTION_NONE == level;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        String[] columns = new String[] {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "reserved1", "reserved2", "reserved3", "REMARKS", "PROCEDURE_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, CHAR, CHAR, CHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "procedures", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
        String[] columns = new String[] {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, INTEGER, VARCHAR, INTEGER, SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "procedure_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        Pattern tableNameRegex = tableNamePattern == null || "".equals(tableNamePattern) ? null : Pattern.compile(tableNamePattern.replace("%", ".*"));

        final Iterable<List<?>> tablesData;
        if (catalog == null) {
            tablesData = tables.entrySet().stream()
                    .flatMap(p -> p.getValue().stream().map(t -> asList(p.getKey(), null, t, "TABLE", null, null, null, null, null, null)))
                    .collect(toList());
        } else {
            tablesData = tables.getOrDefault(catalog, Collections.emptyList()).stream()
                    .filter(t -> tableNameRegex == null || tableNameRegex.matcher(t).matches())
                    .map(t -> asList(catalog, null, t, "TABLE", null, null, null, null, null, null))
                    .collect(toList());
        }

        String[] columns = new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"};
        int[] sqlTypes = new int[columns.length];
        Arrays.fill(sqlTypes, VARCHAR);
        return new ListRecordSet(null, "system", "tables", systemColumns(columns, sqlTypes), tablesData);
    }

    @Override
    public ResultSet getSchemas() {
        return new ListRecordSet(null, "system", "schemas", systemColumns(new String[] {"TABLE_SCHEM", "TABLE_CATALOG"}, new int[] {VARCHAR, VARCHAR}), emptyList());
    }

    @Override
    public ResultSet getCatalogs() {
        Iterable<List<?>> catalogs = getCatalogNames().stream().map(Collections::singletonList).collect(toList());
        return new ListRecordSet(null, "system", "catalogs", systemColumns(new String[] {"TABLE_CAT"}, new int[] {VARCHAR}), catalogs);
    }

    public List<String> getCatalogNames() {
        return catalogs;
    }

    @Override
    public ResultSet getTableTypes() {
        return new ListRecordSet(null, "system", "table_types", systemColumns(new String[] {"TABLE_TYPE"}, new int[] {VARCHAR}), singletonList(singletonList("TABLE")));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Pattern tableNameRegex = tableNamePattern == null || "".equals(tableNamePattern) ? null : Pattern.compile(tableNamePattern.replace("%", ".*"));


        final Iterable<ResultSetMetaData> mds;
        if (catalog == null) {
            mds = tables.entrySet().stream()
                    .flatMap(p -> p.getValue().stream().map(t -> getMetadata(p.getKey(), t)))
                    .collect(toList());
        } else {
            mds = tables.getOrDefault(catalog, Collections.emptyList()).stream()
                    .filter(t -> tableNameRegex == null || tableNameRegex.matcher(t).matches())
                    .map(t -> getMetadata(catalog, t))
                    .collect(toList());
        }

        List<List<?>> result = new ArrayList<>();
        for(ResultSetMetaData md : mds) {
            int n = md.getColumnCount();
            for (int i = 1; i <= n; i++) {
                //TODO: validate whether it is possible to retrieve write-block-size (128K by default) using java client and do it here if possible
                result.add(asList("".equals(tableNamePattern) ? "" : md.getCatalogName(i), null, md.getTableName(1), md.getColumnName(i), md.getColumnType(i), md.getColumnTypeName(i), 0, 0, 0, 0, columnNullable, null, null, md.getColumnType(i), 0, md.getColumnType(i) == VARCHAR ? 128 * 1024 : 0, ordinal(md, md.getColumnName(i)), "YES", md.getCatalogName(i), null, md.getColumnTypeName(i), null, "NO", "NO"));
            }
        }

        String[] columns = new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, INTEGER, SMALLINT, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "columns", systemColumns(columns, sqlTypes), result);
    }

    private int ordinal(ResultSetMetaData md, String columnName) {
        int ordinal = 0;
        try {
            int n = md.getColumnCount();
            for (int i = 1; i <= n; i++) {
                if (columnName.equals(md.getColumnName(i))) {
                    ordinal = i;
                    break;
                }
            }
        } catch (SQLException e) {
            // ignore exception
            // TODO: add logging
        }
        return ordinal;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        String[] columns = new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR,VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "column_privileges", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        String[] columns = new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR,VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "table_privileges", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        String[] columns = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
        int[] sqlTypes = new int[]{SMALLINT, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, SMALLINT, SMALLINT};
        return new ListRecordSet(null, "system", "best_row_identifier", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        String[] columns = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
        int[] sqlTypes = new int[]{SMALLINT, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, SMALLINT, SMALLINT};
        return new ListRecordSet(null, "system", "version_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
        final Iterable<List<?>> tablesData;
        if (catalog == null) {
            tablesData = tables.entrySet().stream()
                    .flatMap(p -> p.getValue().stream().map(t -> asList(p.getKey(), null, t, "PK", 1, "PK")))
                    .collect(toList());
        } else {
            tablesData = tables.getOrDefault(catalog, Collections.emptyList()).stream()
                    .filter(t -> table == null || table.equals(t))
                    .map(t -> asList(catalog, null, t, "PK", 1, "PK"))
                    .collect(toList());
        }

        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
        int[] sqlTypes = new int[]{VARCHAR,VARCHAR,VARCHAR,VARCHAR,SMALLINT,VARCHAR};
        return new ListRecordSet(null, "system", "primary_keys", systemColumns(columns, sqlTypes), tablesData);
    }

    public List<String> getTableNames(String catalog) {
        if (catalog == null) {
            return tables.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        }
        return new ArrayList<>(tables.getOrDefault(catalog, Collections.emptyList()));
    }


    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY",};
        int[] sqlTypes = new int[]{VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,SMALLINT,SMALLINT,SMALLINT,VARCHAR,VARCHAR,SMALLINT};
        return new ListRecordSet(null, "system", "imported_keys", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "exported_keys", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "cross_references", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTypeInfo() {
        String[] columns = new String[] {
                "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE",
                "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
                "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX",
        };


        Iterable<List<?>> data =
                asList(
                        asList("VARCHAR", VARCHAR, 65535, "'", "'", "(M) [CHARACTER SET charset_name] [COLLATE collation_name]", (short)typeNullable,
                                true, (short)typeSearchable, false, false, false, "string",
                                (short)0, (short)0, VARCHAR, 0, 10
                        ),
                        asList("INT", INTEGER, 3, "", "", "[(M)] [UNSIGNED] [ZEROFILL]", (short)typeNullable,
                                true, (short)typeSearchable, false, false, false, "integer",
                                (short)0, (short)0, INTEGER, 0, 10
                        ),
                        asList("DOUBLE", DOUBLE, 22, "", "", "[(M,D)] [UNSIGNED] [ZEROFILL]", (short)typeNullable,
                                true, (short)typeSearchable, false, false, false, "double",
                                (short)0, (short)0, DOUBLE, 0, 10
                        ),
                        asList("BLOB", BLOB, 65535, "", "", "[(M)]", (short)typeNullable,
                                true, (short)typeSearchable, false, false, false, "bytes",
                                (short)0, (short)0, BLOB, 0, 10
                        ),
                        asList("LIST", ARRAY, 0, "", "", "[(M)]", (short)typeNullable,
                                true, (short)typeSearchable, false, false, false, "bytes",
                                (short)0, (short)0, BLOB, 0, 10
                        ),
                        asList("MAP", OTHER, 0, "", "", "[(M)]", (short)typeNullable,
                                true, (short)typeSearchable, false, false, false, "bytes",
                                (short)0, (short)0, OTHER, 0, 10
                        ),
                        asList("JAVA_OBJECT", JAVA_OBJECT, 0, "", "", "[(M)]", (short)typeNullable,
                                true, (short)typeSearchable, false, false, false, "bytes",
                                (short)0, (short)0, JAVA_OBJECT, 0, 10
                        )
                        // TODO: GeoJson
                );


        return new ListRecordSet(null, "system", "table_info", systemColumns(columns), data);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
        final Iterable<List<?>> indicesData;
        if (catalog == null) {
            indicesData = indices.entrySet().stream().flatMap(p -> p.getValue().stream()).map(IndexInfo::asList).collect(Collectors.toList());
        } else {
            indicesData = getOrDefault(indices, catalog, Collections.emptyList()).stream().map(IndexInfo::asList).collect(Collectors.toList());
        }

        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, TINYINT, VARCHAR, VARCHAR, SMALLINT, SMALLINT, VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR};
        return new ListRecordSet(null, "system", "index_info", systemColumns(columns, sqlTypes), indicesData);
    }

    public List<List<?>> getIndexInfo() {
        return indices.entrySet().stream().flatMap(p -> p.getValue().stream()).map(i -> asList(i.set, i.name, i.bin, i.type)).collect(Collectors.toList());
    }

    private ResultSetMetaData getMetadata(String namespace, String table) {
        return SneakyThrower.get(() -> connection.createStatement().executeQuery(format("select * from %s.%s limit %d", namespace, table, discoverMetadataLines)).getMetaData());
    }

    @Override
    public boolean supportsResultSetType(int type) {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        String[] columns = new String[] {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "udt", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        List<List<?>> types = asList(
                asList(catalog, null, "list", null, null, Object.class.getName()),
                asList(catalog, null, "map", null, null, Object.class.getName()),
                asList(catalog, null, "GeoJSON", null, null, Object.class.getName())
        );

        String[] columns = new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "super_types", systemColumns(columns, sqlTypes), types);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "super_tables", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        String[] columns = new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet(null, "system", "attributes", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
    }

    @Override
    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return parseVersion(getDatabaseProductVersion().split("\\.")[0]);
    }

    @Override
    public int getDatabaseMinorVersion() {
        String[] fragments = getDatabaseProductVersion().split("\\.");
        return parseVersion(fragments[fragments.length - 1]);
    }

    private int parseVersion(String str) {
        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 0;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_VALID_FOREVER;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        String[] columns = new String[]{"TABLE_SCHEM", "TABLE_CATALOG"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR};
        return new ListRecordSet(null, "system", "schemas", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() {
        // The driver does not support any properties right now but hopefully will.
        // TODO: do not forget to add properties here once implemented.
        String[] columns = new String[]{"NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION",};
        int[] sqlTypes = new int[]{VARCHAR,INTEGER,VARCHAR,VARCHAR};
        return new ListRecordSet(null, "system", "client_inf_properties", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        List<List<?>> clientFunctions = functionManager.getFunctionNames().stream().map(name -> asList(null, null, name, "Java", functionResultUnknown, name)).collect(toList());
        List<List<?>> luaFunctions = Stream.of("min", "max", "sum", "avg", "sumsqs", "count", "distinct").map(name -> asList(null, null, name, "Lua", functionResultUnknown, name)).collect(toList());

        List<List<?>> functions = new ArrayList<>();
        functions.addAll(clientFunctions);
        functions.addAll(luaFunctions);
        functions.sort(Comparator.comparing(o -> ((String) o.get(2))));

        String[] columns = new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR};
        return new ListRecordSet(null, "system", "functions", systemColumns(columns, sqlTypes), functions);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) {
        // TODO: implement this method: add kind of annotations that describe the functions parameters to JavaScript and Lua code and  parse them here.
        String[] columns = new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"};
        int[] sqlTypes = new int[]{VARCHAR,VARCHAR,VARCHAR,VARCHAR,SMALLINT,INTEGER,VARCHAR,INTEGER,INTEGER,SMALLINT,SMALLINT,SMALLINT,VARCHAR,INTEGER,INTEGER,VARCHAR,VARCHAR};
        return new ListRecordSet(null, "system", "function_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, INTEGER, VARCHAR};
        return new ListRecordSet(null, "system", "pseudo_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        return false;
    }

    private Optional<Manifest> manifest() {
        return iosafe(() -> Collections.list(getClass().getClassLoader().getResources("META-INF/MANIFEST.MF")).stream()
                .map(r -> manifest(stream(r)))
                .filter(m -> AerospikeDriver.class.getSimpleName().equals(m.getMainAttributes().getValue("Name")))
                .findFirst());
    }

    private InputStream stream(URL url) {
        return iosafe(url::openStream);
    }


    private Manifest manifest(InputStream in) {
        return iosafe(() -> new Manifest(in));
    }


    private List<DataColumn> systemColumns(String[] names) {
        return columns("system", null, names);
    }

    private List<DataColumn> columns(String catalog, String table, String[] names) {
        return range(0, names.length).boxed().map(i -> DATA.create(catalog, table, names[i], names[i])).collect(toList());
    }

    private List<DataColumn> systemColumns(String[] names, int[] types) {
        return columns("system", null, names, types);
    }

    private List<DataColumn> columns(String catalog, String table, String[] names, int[] types) {
        return range(0, names.length).boxed().map(i -> DATA.create(catalog, table, names[i], names[i]).withType(types[i])).collect(toList());
    }

    private <R> R iosafe(ThrowingSupplier<R, IOException> supplier) {
        try {
            return supplier.get();
        } catch (IOException e) {
            return SneakyThrower.sneakyThrow(new SQLException(e));
        }
    }

    private class IndexInfo {
        private final String namespace;
        private final String set;
        private final String name;
        private final String bin;
        private final String type;


        private IndexInfo(String namespace, String set, String name, String bin, String type) {
            this.namespace = namespace;
            this.set = set;
            this.name = name;
            this.bin = bin;
            this.type = type;
        }

        public List<?> asList() {
            return Arrays.asList(namespace, null, set, 0, null, name, tableIndexClustered, ordinal(getMetadata(namespace, set), bin), bin, null, null /*TODO number of unique values in index: stat index returns relevant information */, 0, null);
        }
    }
}
