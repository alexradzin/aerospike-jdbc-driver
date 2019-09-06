package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.policy.InfoPolicy;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.ExpressionAwareResultSetFactory;
import com.nosqldriver.sql.ListRecordSet;
import com.nosqldriver.sql.SimpleWrapper;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.jar.Manifest;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.ListRecordSet.discoverTypes;
import static java.lang.String.format;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BLOB;
import static java.sql.Types.CHAR;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class AerospikeDatabaseMetadata implements DatabaseMetaData, SimpleWrapper {
    private final static ConnectionParametersParser parser = new ConnectionParametersParser();
    private final String url;
    private final Properties clientInfo;

    private final Optional<Manifest> manifest;
    private final Map<String, String> dbInfo;
    private final IAerospikeClient client;
    private final Connection connection;
    private final InfoPolicy infoPolicy = new InfoPolicy();
    private static final String newLine = System.lineSeparator();


    public AerospikeDatabaseMetadata(String url, Properties info, IAerospikeClient client, Connection connection) {
        this.url = url;
        clientInfo = parser.clientInfo(url, info);
        this.client = client;
        this.connection = connection;
        manifest = manifest();
        dbInfo = new HashMap<>();
        Arrays.stream(client.getNodes()).forEach(node -> dbInfo.putAll(Info.request(infoPolicy, node)));
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return clientInfo.getProperty("user");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return dbInfo.getOrDefault("edition", "Aerospike");
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return dbInfo.getOrDefault("build", "N/A");
    }

    @Override
    public String getDriverName() throws SQLException {
        return AerospikeDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return manifest.map(m -> (String)m.getMainAttributes().get("Version")).orElse("N/A");
    }

    @Override
    public int getDriverMajorVersion() {
        return manifest.map(m -> (String)m.getMainAttributes().get("Version")).map(v -> Integer.parseInt(v.split("\\.")[0])).orElse(1);
    }

    @Override
    public int getDriverMinorVersion() {
        return manifest.map(m -> (String)m.getMainAttributes().get("Version")).map(v -> v.split("\\.")).map(a -> a.length > 1 ? Integer.parseInt(a[a.length -1]) : 0).get();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return " ";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "sum,avg,min,max,count,len,charIndex,now,year";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "char,concat,left,lower,upper,str,substring,space,reverse";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "year,now";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\"; //TODO: ?
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "namespace";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "lua script";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "namespace";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 14;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 32767;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 32767;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 31;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 63;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        String[] columns = new String[] {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "reserved1", "reserved2", "reserved3", "REMARKS", "PROCEDURE_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, CHAR, CHAR, CHAR, VARCHAR, SMALLINT};
        return new ListRecordSet("system", "procedures", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        String[] columns = new String[] {"PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, INTEGER, VARCHAR, INTEGER, SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR};
        return new ListRecordSet("system", "procedure_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        Pattern tableNameRegex = tableNamePattern == null || "".equals(tableNamePattern) ? null : Pattern.compile(tableNamePattern.replace("%", ".*"));

        Iterable<List<?>> tables =
                getTablesData(catalog)
                        .filter(p -> catalog == null || catalog.equals(p.getProperty("ns")))
                        .filter(p -> tableNameRegex == null || tableNameRegex.matcher(p.getProperty("set")).matches())
                        .map(p -> asList("".equals(tableNamePattern) ? "" : p.getProperty("ns"), null, p.getProperty("set"), "TABLE", null, null, null, null, null, null))
                        .collect(toList());

        String[] columns = new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"};
        int[] sqlTypes = new int[columns.length];
        Arrays.fill(sqlTypes, VARCHAR);
        return new ListRecordSet("system", "tables", systemColumns(columns, sqlTypes), tables);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return new ListRecordSet("system", "schemas", systemColumns(new String[] {"TABLE_SCHEM", "TABLE_CATALOG"}, new int[] {VARCHAR, VARCHAR}), emptyList());
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        Iterable<List<?>> catalogs = getCatalogNames().stream().map(Collections::singletonList).collect(toList());
        return new ListRecordSet("system", "catalogs", systemColumns(new String[] {"TABLE_CAT"}, new int[] {VARCHAR}), catalogs);
    }

    private List<String> getCatalogNames() {
        return Arrays.stream(client.getNodes())
                .map(node -> Info.request(infoPolicy, node, "namespaces"))
                .map(str -> str.split(";"))
                .map(Arrays::asList)
                .flatMap(Collection::stream)
                .distinct()
                .sorted()
                .collect(toList());
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return new ListRecordSet("system", "table_types", systemColumns(new String[] {"TABLE_TYPE"}, new int[] {VARCHAR}), singletonList(singletonList("TABLE")));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Pattern tableNameRegex = tableNamePattern == null || "".equals(tableNamePattern) ? null : Pattern.compile(tableNamePattern.replace("%", ".*"));

        Iterable<ResultSetMetaData> mds =
                getTablesData(catalog)
                        .filter(p -> catalog == null || catalog.equals(p.getProperty("ns")))
                        .filter(p -> tableNameRegex == null || tableNameRegex.matcher(p.getProperty("set")).matches())
                        .map(p -> getMetadata(p.getProperty("ns"), p.getProperty("set")))
                        .collect(toList());

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
        return new ListRecordSet("system", "columns", systemColumns(columns, sqlTypes), result);
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
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        String[] columns = new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR,VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet("system", "column_privileges", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        String[] columns = new String[] {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR,VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet("system", "table_privileges", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        String[] columns = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
        int[] sqlTypes = new int[]{SMALLINT, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, SMALLINT, SMALLINT};
        return new ListRecordSet("system", "best_row_identifier", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        String[] columns = new String[]{"SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"};
        int[] sqlTypes = new int[]{SMALLINT, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, SMALLINT, SMALLINT};
        return new ListRecordSet("system", "version_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        Iterable<List<?>> tables =
                getTablesData(catalog)
                        .filter(p -> catalog == null || catalog.equals(p.getProperty("ns")))
                        .filter(p -> table == null || table.equals(p.getProperty("set")))
                        .map(p -> asList(p.getProperty("ns"), null, p.getProperty("set"), "PK", 1, "PK"))
                        .collect(toList());

        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
        int[] sqlTypes = new int[]{VARCHAR,VARCHAR,VARCHAR,VARCHAR,SMALLINT,VARCHAR};
        return new ListRecordSet("system", "primary_keys", systemColumns(columns, sqlTypes), tables);
    }


    private Stream<Properties> getTablesData(String catalog) {
        return getInfo(catalog == null ? "sets" : format("sets/%s", catalog));
    }

    private Stream<Properties> getInfo(String command) {
        return Arrays.stream(client.getNodes())
                .map(node -> Info.request(infoPolicy, node, command))
                .filter(Objects::nonNull)
                .map(s -> s.split(";"))
                .flatMap(Arrays::stream)
                .map(s -> s.replace(":", newLine))
                .map(s -> {
                    Properties props = new Properties();
                    try {
                        props.load(new StringReader(s));
                        return props;
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                });
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY",};
        int[] sqlTypes = new int[]{VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,SMALLINT,SMALLINT,SMALLINT,VARCHAR,VARCHAR,SMALLINT};
        return new ListRecordSet("system", "imported_keys", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet("system", "exported_keys", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        String[] columns = new String[]{"PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, SMALLINT, SMALLINT, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet("system", "cross_references", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
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
                        )
                        // TODO: list, map, GeoJson
                );


        return new ListRecordSet("system", "table_info", systemColumns(columns, discoverTypes(columns.length, data)), data);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        Iterable<List<?>> indexes =
                getInfo("sindex-list:")
                        .filter(p -> catalog == null || catalog.equals(p.getProperty("ns")))
                        .filter(p -> table == null || table.equals(p.getProperty("set")))
                        .map(p -> asList(p.getProperty("ns"), null, p.getProperty("set"), 0, null, p.getProperty("indexname"), tableIndexClustered, ordinal(getMetadata(p.getProperty("ns"), p.getProperty("set")), p.getProperty("bin")), p.getProperty("bin"), null, null /*TODO number of unique values in index: stat index returns relevant information */, 0, null))
                        .collect(toList());


        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, TINYINT, VARCHAR, VARCHAR, SMALLINT, SMALLINT, VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR};
        return new ListRecordSet("system", "index_info", systemColumns(columns, sqlTypes), indexes);
    }

    private ResultSetMetaData getMetadata(String namespace, String table) {
        try {
            return connection.createStatement().executeQuery(format("select * from %s.%s limit 1", namespace, table)).getMetaData();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        String[] columns = new String[] {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, SMALLINT};
        return new ListRecordSet("system", "udt", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        List<List<?>> types = asList(
                asList(catalog, null, "list", null, null, Object.class.getName()),
                asList(catalog, null, "map", null, null, Object.class.getName()),
                asList(catalog, null, "GeoJSON", null, null, Object.class.getName())
        );

        String[] columns = new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet("system", "super_types", systemColumns(columns, sqlTypes), types);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR};
        return new ListRecordSet("system", "super_tables", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        String[] columns = new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT};
        return new ListRecordSet("system", "attributes", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        try {
            return Integer.parseInt(getDatabaseProductVersion().split("\\.")[0]);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        try {
            String[] fragments = getDatabaseProductVersion().split("\\.");
            return Integer.parseInt(fragments[fragments.length - 1]);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_VALID_FOREVER;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String[] columns = new String[]{"TABLE_SCHEM", "TABLE_CATALOG"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR};
        return new ListRecordSet("system", "schemas", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        // The driver does not support any properties right now but hopefully will.
        // TODO: do not forget to add properties here once implemented.
        String[] columns = new String[]{"NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION",};
        int[] sqlTypes = new int[]{VARCHAR,INTEGER,VARCHAR,VARCHAR};
        return new ListRecordSet("system", "client_inf_properties", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        List<List<?>> jsFunctions = new ExpressionAwareResultSetFactory().getClientSideFunctionNames().stream().map(name -> asList(null, null, name, "JavaScript", functionResultUnknown, name)).collect(toList());
        List<List<?>> luaFunctions = Stream.of("min", "max", "sum", "avg", "count", "distinct").map(name -> asList(null, null, name, "Lua", functionResultUnknown, name)).collect(toList());

        List<List<?>> functions = new ArrayList<>();
        functions.addAll(jsFunctions);
        functions.addAll(luaFunctions);
        functions.sort(Comparator.comparing(o -> ((String) o.get(2))));

        String[] columns = new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, SMALLINT, VARCHAR};
        return new ListRecordSet("system", "functions", systemColumns(columns, sqlTypes), functions);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        // TODO: implement this method: add kind of annotations that describe the functions parameters to JavaScript and Lua code and  parse them here.
        String[] columns = new String[]{"FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"};
        int[] sqlTypes = new int[]{VARCHAR,VARCHAR,VARCHAR,VARCHAR,SMALLINT,INTEGER,VARCHAR,INTEGER,INTEGER,SMALLINT,SMALLINT,SMALLINT,VARCHAR,INTEGER,INTEGER,VARCHAR,VARCHAR};
        return new ListRecordSet("system", "function_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String[] columns = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE"};
        int[] sqlTypes = new int[]{VARCHAR, VARCHAR, VARCHAR, VARCHAR, INTEGER, INTEGER, INTEGER, INTEGER, VARCHAR, VARCHAR, INTEGER, VARCHAR};
        return new ListRecordSet("system", "pseudo_columns", systemColumns(columns, sqlTypes), emptyList());
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    private Optional<Manifest> manifest() {
        try {
            return Collections.list(getClass().getClassLoader().getResources("META-INF/MANIFEST.MF")).stream()
                    .map(r -> manifest(stream(r)))
                    .filter(m -> AerospikeDriver.class.getSimpleName().equals(m.getMainAttributes().getValue("Name")))
                    .findFirst();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private InputStream stream(URL url) {
        try {
            return url.openStream();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }


    private Manifest manifest(InputStream in) {
        try {
            return new Manifest(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<DataColumn> systemColumns(String[] names, int[] types) {
        return columns("system", null, names, types);
    }

    private List<DataColumn> columns(String catalog, String table, String[] names, int[] types) {
        return range(0, names.length).boxed().map(i -> DATA.create(catalog, table, names[i], names[i]).withType(types[i])).collect(toList());
    }
}
