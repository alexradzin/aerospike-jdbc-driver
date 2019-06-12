/**
 * This package contains tests that can be qualified as integration rather than unit tests because they peform real
 * SQL requests agains real Aerospike DB running locally using the JDBC driver developled here.
 *
 * The test are grouped in test cases by the main SQL statement (SELECT, UPDATE, DELETE, INSERT etc.) as well as
 * by the data required for the test. This is done for performance reasons. There are a lot of read-only tests
 * that do not change data in database. Read only tests that need the same data are grouped together, so that the DB
 * is filled once in the beginning of the test case.
 */
package com.nosqldriver.aerospike.sql;
