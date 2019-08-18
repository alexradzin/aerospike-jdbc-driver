# Aerospike JDBC Driver 

[![Build Status](https://travis-ci.com/alexradzin/aerospike-jdbc-driver.svg?branch=master)](https://travis-ci.com/alexradzin/aerospike-jdbc-driver)
[![codecov](https://codecov.io/gh/alexradzin/aerospike-jdbc-driver/branch/master/graph/badge.svg)](https://codecov.io/gh/alexradzin/aerospike-jdbc-driver)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/63282ca8b0ff451ba30fea499b32408f)](https://www.codacy.com/app/alexradzin/aerospike-jdbc-driver?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=alexradzin/aerospike-jdbc-driver&amp;utm_campaign=Badge_Grade)

## Motivation

Simplify access to Aerospike DB.

## Introduction

[Aerospike](https://www.aerospike.com/) is extremely fast and scalable key-value store. It provides clients for various languages including Java. However unlike relational databases that all support SQL and therefore can easily implement JDBC driver each no-SQL DB creates its own world. API exposed by clients of no-SQL databases are different (not standard) and optimized for specific features of certain databases. 

From other hand threre are a lot of tools that help to visualize data stored in database, create reports based on the data, perform various ETL operations etc. These tools traditionally support SQL. Database that has standards complient JDBC driver can be easilty connected to various tools. Majority of popular no-SQL databased have JDBC drivers. Aerospike did not have one. This was the reason to start this project. 

## Key Features

  * Use either a single all-in-one JAR or define dependency (TBD)
  * Use standard SQL to insert, update, retrieve and delete data to/from Aerospike DB
  * Compliant with JDBC JDBC 4.0.
  * Codeless integration with popular BI, reporting, and ETL tools.
  * Use standard JDBC API from any JVM compatible language

## Quick start

The driver is being developed now and is not available in any public repository. One can however build and publish it to a private repository. Once it is done refer to it using definition like the following:

For maven:
```xml
<dependency>
    <groupId>com.nosqldriver</groupId>
    <artifactId>aerospike-jdbc-driver</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

or for Gradle:
```gradle
compile group: 'com.nosqldriver', name: 'aerospike-jdbc-driver', version: '1.0-SNAPSHOT'
```

It is very easy to build the project. The only prerequisite is JDK 8 or higher. Clone git repository and run

```sh
./gradlew build -x test
```

This command will compile and project and  package jar that can be found under ./build/lib
If  you  want to run unit test you want to run unit tests please [install and run aerospike database](https://www.aerospike.com/docs/operations/install/) on machine that runs the tests. Once this is done run 

```sh
./gradlew build
```
This command will compile the code, run all unit and integration tests and create jar file. 

Aerospike JDBC driver depends on SQL parser and [Aerospke Java client](https://www.aerospike.com/docs/client/java/). If you want to use the driver with 3rd party tool it is very convenient to build single fat jar that contains all dependencies. To get it run the following command:

```sh
./gradlew fatJar
```
Fat  JAR `aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar` can be added to 3rd party tool like [SQuirreL](http://squirrel-sql.sourceforge.net/) or [DB viewer](https://dbeaver.io/)

### Coding with the driver

The Aerospike JDBC driver is complient to JDBC version 4. If it is available in classpath no more configuration is required and the following code should work:

```java
Connection conn = DriverManager.getConnection("jdbc:aerospike:localhost/test");
ResultSet rs = conn.createStatement().executeQuery("select * from people");
while (rs.next()) {
    rs.getString(1);
    rs.getInt("year_of_birth");
}
```

## SQL compliance

### Supported statements

  * insert, update, delete, select
  * Complex where clause can be used with update, delete and select. Use special field "PK" to refere to the primary key. Syntanctically there is no difference between referencing to primary key or any data column. Select statement does its best effort to use secondary indexes if defined. 
  * Nested selects are supported. 
  * Select can be used with distinct, group by, order by.

### Statements that will be supported in future

  * create/drop index
  * drop table
  * describe
  * show schemas/tables/indexes

### Statements that will not be supported

  * create table. This operation is meaningless applicable to Aerospike that creates set once somebody writes to this set. 
  * create/drop schema just cannot be implemented for Aerospike that requires static definition of namespaces using `aerospike.conf`.

### Functions

| Function                       | Description                                                              |
| -------------------------------| ------------------------------------------------------------------------ |
| len(s)                         | length of string                                                         |
| ascii(s)                       | ASCII code of the first character of the given string                    |
| char(code)                     | character by its ASCII code                                              |
| charIndex(subStr, str, start)  | starting position of `subStr` into `str` starting from `start` index     |
| concat(...)                    | concatenate given string                                                 |
| left(str, n)                   | n-characters sustring of given string from the beginning                 |
| lower(str, n)                  | converts all characters to loewer case                                   |
| upper(str, n)                  | converts all characters to upper case                                    |
| str(n)                         | converts given number to string                                          |
| substring(str, start, length)  | `length` characters long substring of given string started from `start`  |
| space(n)                       | generates string that consists of `n` spaces                             |
| reverse(str)                   | reverses given string                                                    |
| now()                          | retrieves current epoch time in milliseconds                             |
| year()                         | retrueves current year                                                   |
