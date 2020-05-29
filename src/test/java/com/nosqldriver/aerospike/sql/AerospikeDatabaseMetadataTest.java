package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeTestUrl;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getColumnValues;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.DatabaseMetaData.sqlStateSQL;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AerospikeDatabaseMetadataTest {
    private Connection testConn = getTestConnection();
    @Test
    void trivialFlags() throws SQLException {
        DatabaseMetaData md = testConn.getMetaData();
        assertFalse(md.allProceduresAreCallable());
        assertTrue(md.allTablesAreSelectable());
        assertFalse(md.isReadOnly());
        assertFalse(md.nullsAreSortedHigh());
        assertTrue(md.nullsAreSortedLow());
        assertTrue(md.nullsAreSortedAtStart()); //???
        assertFalse(md.nullsAreSortedAtEnd());
        assertFalse(md.usesLocalFiles());
        assertFalse(md.usesLocalFilePerTable());
        assertFalse(md.supportsMixedCaseIdentifiers());
        assertFalse(md.storesUpperCaseIdentifiers());
        assertFalse(md.storesLowerCaseIdentifiers());
        assertFalse(md.storesMixedCaseIdentifiers());
        assertFalse(md.supportsMixedCaseQuotedIdentifiers());
        assertFalse(md.storesUpperCaseQuotedIdentifiers());
        assertFalse(md.storesLowerCaseQuotedIdentifiers());
        assertFalse(md.storesMixedCaseQuotedIdentifiers());
        assertFalse(md.supportsAlterTableWithAddColumn());
        assertFalse(md.supportsAlterTableWithDropColumn());
        assertFalse(md.supportsColumnAliasing());
        assertFalse(md.nullPlusNonNullIsNull());
        assertFalse(md.supportsConvert());
        assertFalse(md.supportsTableCorrelationNames());
        assertFalse(md.supportsDifferentTableCorrelationNames());
        assertFalse(md.supportsExpressionsInOrderBy());
        assertFalse(md.supportsOrderByUnrelated());
        assertTrue(md.supportsGroupBy());
        assertFalse(md.supportsGroupByUnrelated());
        assertFalse(md.supportsGroupByBeyondSelect());
        assertFalse(md.supportsLikeEscapeClause());
        assertFalse(md.supportsMultipleResultSets());
        assertFalse(md.supportsMultipleTransactions());
        assertFalse(md.supportsNonNullableColumns());
        assertTrue(md.supportsMinimumSQLGrammar());
        assertFalse(md.supportsCoreSQLGrammar());
        assertFalse(md.supportsExtendedSQLGrammar());
        assertFalse(md.supportsANSI92EntryLevelSQL());
        assertFalse(md.supportsANSI92IntermediateSQL());
        assertFalse(md.supportsANSI92FullSQL());
        assertFalse(md.supportsIntegrityEnhancementFacility());
        assertTrue(md.supportsOuterJoins());
        assertFalse(md.supportsFullOuterJoins());
        assertTrue(md.supportsLimitedOuterJoins());
        assertTrue(md.isCatalogAtStart());
        assertFalse(md.supportsSchemasInDataManipulation());
        assertFalse(md.supportsSchemasInProcedureCalls());
        assertFalse(md.supportsSchemasInTableDefinitions());
        assertFalse(md.supportsSchemasInIndexDefinitions());
        assertFalse(md.supportsSchemasInPrivilegeDefinitions());
        assertTrue(md.supportsCatalogsInDataManipulation());
        assertFalse(md.supportsCatalogsInProcedureCalls());
        assertTrue(md.supportsCatalogsInTableDefinitions());
        assertTrue(md.supportsCatalogsInIndexDefinitions());
        assertFalse(md.supportsCatalogsInPrivilegeDefinitions());
        assertFalse(md.supportsPositionedDelete());
        assertFalse(md.supportsPositionedUpdate());
        assertFalse(md.supportsSelectForUpdate());
        assertFalse(md.supportsStoredProcedures());
        assertFalse(md.supportsSubqueriesInComparisons());
        assertFalse(md.supportsSubqueriesInExists());
        assertFalse(md.supportsSubqueriesInIns());
        assertFalse(md.supportsSubqueriesInQuantifieds());
        assertFalse(md.supportsCorrelatedSubqueries());
        assertFalse(md.supportsUnion());
        assertFalse(md.supportsUnionAll());
        assertFalse(md.supportsOpenCursorsAcrossCommit());
        assertFalse(md.supportsOpenCursorsAcrossRollback());
        assertFalse(md.supportsOpenStatementsAcrossCommit());
        assertFalse(md.supportsOpenStatementsAcrossRollback());
        assertFalse(md.doesMaxRowSizeIncludeBlobs());
        assertFalse(md.supportsTransactions());
        assertFalse(md.supportsDataDefinitionAndDataManipulationTransactions());
        assertFalse(md.supportsDataManipulationTransactionsOnly());
        assertFalse(md.dataDefinitionCausesTransactionCommit());
        assertFalse(md.dataDefinitionIgnoredInTransactions());
        assertFalse(md.supportsBatchUpdates());
        assertFalse(md.supportsSavepoints());
        assertFalse(md.supportsNamedParameters());
        assertFalse(md.supportsMultipleOpenResults());
        assertFalse(md.supportsGetGeneratedKeys());
        assertFalse(md.locatorsUpdateCopy());
        assertFalse(md.supportsStatementPooling());
        assertFalse(md.supportsStoredFunctionsUsingCallSyntax());
        assertFalse(md.autoCommitFailureClosesAllResultSets());
        assertFalse(md.generatedKeyAlwaysReturned());
    }



    @Test
    void trivialInt() throws SQLException {
        DatabaseMetaData md = testConn.getMetaData();
        // Driver version is taken from Manifest and is not available when running from IDE
        assertTrue(md.getDriverMajorVersion() >= 0);
        assertTrue(md.getDriverMinorVersion() >= 0);
        assertEquals(14, md.getMaxBinaryLiteralLength());
        assertEquals(0, md.getMaxCharLiteralLength());
        assertEquals(14, md.getMaxColumnNameLength());
        assertEquals(0, md.getMaxColumnsInGroupBy());
        assertEquals(1, md.getMaxColumnsInIndex());
        assertEquals(0, md.getMaxColumnsInOrderBy());
        assertEquals(32767, md.getMaxColumnsInSelect());
        assertEquals(32767, md.getMaxColumnsInTable());
        assertEquals(0, md.getMaxConnections());
        assertEquals(0, md.getMaxCursorNameLength());
        assertEquals(256, md.getMaxIndexLength());
        assertEquals(14, md.getMaxSchemaNameLength());
        assertEquals(0, md.getMaxProcedureNameLength());
        assertEquals(14, md.getMaxCatalogNameLength());
        assertEquals(8 * 1024 * 1024, md.getMaxRowSize());
        assertEquals(0, md.getMaxStatementLength());
        assertEquals(0, md.getMaxStatements());
        assertEquals(63, md.getMaxTableNameLength());
        assertEquals(0, md.getMaxTablesInSelect());
        assertEquals(63, md.getMaxUserNameLength());
        assertEquals(TRANSACTION_NONE, md.getDefaultTransactionIsolation());
        assertEquals(HOLD_CURSORS_OVER_COMMIT, md.getResultSetHoldability());
        assertTrue(md.getDatabaseMajorVersion() >= 4);
        assertTrue(md.getDatabaseMinorVersion() >= 0);
        assertEquals(4, md.getJDBCMajorVersion());
        assertEquals(0, md.getJDBCMinorVersion());
        assertEquals(sqlStateSQL, md.getSQLStateType());
        assertEquals(RowIdLifetime.ROWID_VALID_FOREVER, md.getRowIdLifetime()); // TODO: check whether this is correct
    }


    @Test
    void trivialString() throws SQLException {
        DatabaseMetaData md = testConn.getMetaData();
        assertEquals(aerospikeTestUrl, md.getURL());
        assertNull(md.getUserName());
        assertEquals("Aerospike Community Edition", md.getDatabaseProductName());
        assertTrue(Integer.parseInt(md.getDatabaseProductVersion().split("\\.")[0]) >= 4);
        assertEquals(AerospikeDriver.class.getName(), md.getDriverName());
        assertNotEquals("", md.getDriverVersion()); // taken from manifest and is not available in dev environment
        assertEquals("\"", md.getIdentifierQuoteString());
        assertEquals("", md.getSQLKeywords());
        assertTrue(md.getNumericFunctions().contains("sum"));
        assertTrue(md.getStringFunctions().contains("concat"));
        assertEquals("", md.getSystemFunctions());
        assertTrue(md.getTimeDateFunctions().contains("now"));
        assertEquals("\\", md.getSearchStringEscape());
        assertEquals("", md.getExtraNameCharacters());
        assertEquals("namespace", md.getSchemaTerm());
        assertEquals("lua script", md.getProcedureTerm());
        assertEquals("namespace", md.getCatalogTerm());
        assertEquals(".", md.getCatalogSeparator());

    }

    @Test
    void connection() throws SQLException {
        assertSame(testConn, testConn.getMetaData().getConnection());
   }


   @Test
   void parameterizedProperties() throws SQLException {
       DatabaseMetaData md = testConn.getMetaData();
       assertFalse(md.supportsConvert(1, 1)); //TODO: implement this

       assertTrue(md.supportsTransactionIsolationLevel(TRANSACTION_NONE));
       assertFalse(md.supportsTransactionIsolationLevel(TRANSACTION_READ_UNCOMMITTED));

       assertTrue(md.supportsResultSetHoldability(HOLD_CURSORS_OVER_COMMIT));
       assertFalse(md.supportsResultSetHoldability(CLOSE_CURSORS_AT_COMMIT));


       assertFalse(md.supportsResultSetType(0));
       assertFalse(md.supportsResultSetConcurrency(0, 0));
       assertFalse(md.ownUpdatesAreVisible(0));
       assertFalse(md.ownDeletesAreVisible(0));
       assertFalse(md.ownInsertsAreVisible(0));
       assertFalse(md.othersUpdatesAreVisible(0));
       assertFalse(md.othersDeletesAreVisible(0));
       assertFalse(md.othersInsertsAreVisible(0));
       assertFalse(md.updatesAreDetected(0));
       assertFalse(md.deletesAreDetected(0));
       assertFalse(md.insertsAreDetected(0));


   }


    //TODO implement separate tests with relevant validations for each result set.
    //TODO remove all indexes before running this test
    @Test
    void resultSets() throws SQLException {
        DatabaseMetaData md = testConn.getMetaData();
        assertResultSet(md.getSchemas(), false); //namespaces are treated as catalogs
        assertResultSet(md.getCatalogs(), true);
        assertResultSet(md.getTableTypes(), true);
        assertResultSet(md.getTypeInfo(), true);
        assertResultSet(md.getClientInfoProperties(), false);

        Collection<String> catalogs = new ArrayList<>();
        catalogs.add(null);
        catalogs.addAll(getColumnValues(md.getCatalogs(), rs -> rs.getString("TABLE_CAT")));

        for (String catalog : catalogs) {
            assertResultSet(md.getProcedures(catalog, null, null), false);
            assertResultSet(md.getProcedureColumns(catalog, null, null, null), false);
            assertResultSet(md.getTables(catalog, null, null, null), null); // not sure that tables exist on build machine when this test is running.
            assertResultSet(md.getColumns(catalog, null, null, null), null); // not sure that tables exist on build machine when this test is running.
            assertResultSet(md.getColumnPrivileges(catalog, null, null, null), false);
            assertResultSet(md.getTablePrivileges(catalog, null, null), false);
            assertResultSet(md.getBestRowIdentifier(catalog, null, null, 0, true), false);
            assertResultSet(md.getVersionColumns(catalog, null, null), false);
            assertResultSet(md.getPrimaryKeys(catalog, null, null), null);  // not sure that tables exist on build machine when this test is running.
            assertResultSet(md.getImportedKeys(catalog, null, null), false);
            assertResultSet(md.getExportedKeys(catalog, null, null), false);
            assertResultSet(md.getCrossReference(catalog, null, null, null, null, null), false);
            assertResultSet(md.getIndexInfo(catalog, null, null, true, true), false);
            assertResultSet(md.getUDTs(catalog, null, null, new int[0]), false);
            assertResultSet(md.getSuperTypes(catalog, null, null), true);
            assertResultSet(md.getSuperTables(catalog, null, null), false);
            assertResultSet(md.getAttributes(catalog, null, null, null), false);
            assertResultSet(md.getSchemas(catalog, null), false);
            assertResultSet(md.getFunctions(catalog, null, null), true);
            assertResultSet(md.getFunctionColumns(catalog, null, null, null), false);
            assertResultSet(md.getPseudoColumns(catalog, null, null, null), false);
        }
    }


    private void assertResultSet(ResultSet rs, Boolean hasData) throws SQLException {
        assertNotNull(rs);
        assertNotNull(rs.getMetaData());
        assertNull(rs.getStatement());
        if (hasData != null) {
            assertEquals(hasData, rs.next());
        }
    }
}