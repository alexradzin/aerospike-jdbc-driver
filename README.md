# Aerospike JDBC Driver 

[![Build Status](https://travis-ci.com/alexradzin/aerospike-jdbc-driver.svg?branch=master)](https://travis-ci.com/alexradzin/aerospike-jdbc-driver)
[![codecov](https://codecov.io/gh/alexradzin/aerospike-jdbc-driver/branch/master/graph/badge.svg)](https://codecov.io/gh/alexradzin/aerospike-jdbc-driver)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/63282ca8b0ff451ba30fea499b32408f)](https://www.codacy.com/app/alexradzin/aerospike-jdbc-driver?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=alexradzin/aerospike-jdbc-driver&amp;utm_campaign=Badge_Grade)

## Motivation

Simplify access to Aerospike DB.

## Introduction

[Aerospike](https://www.aerospike.com/) is extremely fast and scalable key-value store. It provides clients for various languages including Java. However, unlike relational databases that all support SQL and therefore can easily implement JDBC driver each no-SQL DB creates its own world. API exposed by clients of no-SQL databases are different (not standard) and optimized for specific features of certain databases. 

From other hand there are a lot of tools that help to visualize data stored in database, create reports based on the data, perform various ETL operations etc. These tools traditionally support SQL. Database that has standards compliant JDBC driver can be easily connected to various tools. Majority of popular no-SQL databased have JDBC drivers. Aerospike did not have one. This was the reason to start this project. 

## Presentation

Slides for presentation can be found [here.](https://docs.google.com/presentation/d/1McxNantOIO51rEwuh-ns_PLupZkAGJjtsS9SDhkpboc/edit?usp=sharing)

## Key Features
*   Use either a single all-in-one JAR or define dependency (TBD)
*   Use standard SQL to insert, update, retrieve and delete data to/from Aerospike DB
*   Compliant with JDBC 4.0.
*   Codeless integration with popular BI, reporting, and ETL tools.
*   Use standard JDBC API from any JVM compatible language

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

It is very easy to build the project. The only prerequisite is JDK 8 or higher. Clone git repository and run:

```sh
./gradlew build -x test
```

This command will compile and project and  package jar that can be found under ./build/lib
If  you  want to run unit test you want to run unit tests please [install and run aerospike database](https://www.aerospike.com/docs/operations/install/) on the machine that runs the tests. Once this is done run: 

```sh
./gradlew build
```
This command will compile the code, run all unit and integration tests and create jar file.
By default, tests run against Aerospike instance running on `localhost` and listening to default port 3000.
To change this behaviour use system properties `aerospike.host` and `aerospike.port` respectively, e.g.:

```sh
./gradlew build -Daerospike.host=10.10.1.1 -Daerospike.port=3333
```

Aerospike JDBC driver depends on SQL parser and [Aerospke Java client](https://www.aerospike.com/docs/client/java/). If you want to use the driver with 3rd party tool it is very convenient to build single fat jar that contains all dependencies. Create the fat jar using command:

```sh
./gradlew fatJar
```
Fat  JAR `aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar` can be added to 3rd party tool like [SQuirreL](http://squirrel-sql.sourceforge.net/) or [DB viewer](https://dbeaver.io/)

### Coding with the driver

The Aerospike JDBC driver is compliant to JDBC version 4. If it is available in classpath no more configuration is needed, and the following code should work:

```java
Connection conn = DriverManager.getConnection("jdbc:aerospike:localhost/test");
ResultSet rs = conn.createStatement().executeQuery("select * from people");
while (rs.next()) {
    rs.getString(1);
    rs.getInt("year_of_birth");
}
```

### JDBC URL
The JDBC URL format is: `jdbc:aerospike:HOST[:PORT][/NAMESPACE][?PARAM1=VALUE1[&PARAM2=VALUE2]`.
For example `jdbc:aerospike:localhost` connects to Aerospike running on a local machine and listening to the default port (3000). If you want to customize port use URL like `jdbc:aerospike:localhost:4000`, to connect to specific namespace add it to URL like: `jdbc:aerospike:localhost/test`. The following example shows how to connect to namespace `test` of Aerospike running on a remote machine and listening to port 4567: `jdbc:aerospike:10.1.1.1:4567/test`.

Typical Aerospike installation consists of several instances, so several IP addresses of servers can be passed to driver as following: `jdbc:aerospike:first:3100,second:3200,third:3300`. If port is omitted, the default value of 3000 is used.

#### JDBC URL parameters
The parameters can be supplied either as a part of the URL or as the separate properties. The driver translates parameters into Aerospike client policies. Aerospike client has the following policy types:
*   read
*   query
*   scan
*   batch
*   write
*   info

Policy related parameters look like: `policy.POLICY_TYPE.PROPERTY_NAME=PROPERTY_VALUE`, i.e.
*   policy.read.socketTimeout=10000
*   policy.write.generationPolicy=EXPECT_GEN_EQUAL

Some policies have the same properties. For example `socketTimeout`, `totalTimeout`, `replica` etc. are relevant for most policies. If you want to set  the same value for specific property of all policies use `*` instead of policy type: `policy.*.socketTimeout=15000`

## Primary Key (PK)
Primary key in a relational database is a constraint applied to "regular" data column or several columns. Primary key (or just a key) in no-sql databases like Aerospike is special entity that is treated separately from the data. Aerospike JDBC driver hides these differences as much as it is possible emulating behavior of a relational DB. However there are some limitations that should be taking into account. 

Aerospike does not operate with key itself but with its digest that is always calculated. By default, Aerospke does not store the primary key (PK) in the DB, so it cannot be retrieved later. If you want to store key in Aerospike set `sendKey` property of `WritePolicy` to `true` (it is `false` by default). The corresponding property of this driver is `policy.write.sendKey=true`. If you want to retrieve key from the DB you have to use the same property to one of `ReadPolicy`, `QueryPolicy`, `ScanPolicy` or `BatchPolicy` depending on the API you are using. This driver hides from you details of the API used under the hood, if you always want to read the keys set property `sendKey=true` to all policies used for reading (read, query, scan, batch). 

If keys should be always available the easiest way is to supply property `policy.*.sendKey=true`. This statement will be applied to all relevant policies, so Aerospike will always store and retrieve the key. 

### Primary Key Digest (PK_DIGEST)
Even if we do not store or retrieve primary key we can read its digest. Use special field `PK_DIGEST` for this purpose.
This feature can be enabled via parameter `policy.driver.sendKeyDigest=true`.   

## Metadata
Relational databases hold metadata that describe the database structure (catalogs, schemas, tables, columns etc). Aerospike is a schema-less DB. Its tables are called sets and columns are called bins. Set holds any number of rows. Each row can hold any number of bins of any name and type. However, very often people just hold the DB schema in the application layer and in fact each row has the same bins.

JDBC standard operates with 2 types of metadata: 
 - Database schema represented by interface `java.sql.DatabaseMetaData`
 - Table schema represented by interface `java.sql.ResultSetMetaData`

### Database schema
Discovery of metadata of Aerospike cluster may be pretty heavy: it merges information retrieved from each node of the cluster. Various tools call method `Connection.getMetaData()` very often that can cause performance problems. The driver caches this information for certain time period that can be configured using property `policy.driver.databaseMetadataCacheTimeout` (default value is 60000 ms.)
Execution of operation that can change the metadata (`INSERT`, `UPDATE`, `CREATE_INDEX`, `DROP_INDEX`) invalidates this cache.   

### Table schema
The Aerospike JDBC driver discovers schema dynamically using the first `N` rows of the set. This means that if other rows have additional bins they could be ignored when reading data using `select` statement. Number of rows used for the schema discovery can be configured using property `policy.driver.discoverMetadataLines`. Its default value is 1.  

## Export/Import
The driver does not implement import and export functionality. However, various tools (e.g. [DBeaver](https://dbeaver.io/)) does this. Tools typically perform export using query like the following: 

```sql
select * from THE_TABLE
```
The exported results can be stored using various formats (CSV, JSON, XML, SQL insert statements)
If you want to use exported data for import you have to:
*   store the key in the DB
*   configure policy that makes driver to retrieve the key values
(see chapter "Primary Key (PK)" for details).

General recommendation: if you want to store and retrieve keys via the driver always configure connection with property `policy.*.sendKey=true`.

The next issue is the export format. CSV format does not hold any schema information. If CSV file contains column with value 123 we cannot know whether this is integer, double or string, so generic tools cannot generate correct SQL `insert` statement. So, you cannot import data from CSV file to empty table. If this is your scenario create one syntactically correct fake record using `insert` statement. This will help tool to discover the table structure and generate correct SQL `insert` statement. Then import data from the file. After that you can delete the fake record using SQL `delete` statement. If the data file format does not matter export data as SQL insert statements. This script can be executed against empty table because schema discovery is not needed in the case. 

## SQL compliance

### Supported statements
*   insert, update, delete, select, truncate
*   Complex where clause can be used with update, delete and select. Use the special field "PK" to refere to the primary key. Syntactically there is no difference between referencing to primary key or any data column. Select statement does its best effort to use secondary indexes if defined. 
*   Nested selects are supported. 
*   Select can be used with distinct, group by, order by.
*   create/drop index
*   use `namespace_name` to change active namespace. Useful for the interactive mode or scripts. 
*   show catalogs/schemas/tables/indexes

### Statements that will be supported in future
*   describe

### Statements that will not be supported
*   `create table`. This operation is meaningless applicable to Aerospike that creates set once somebody writes to this set. 
*   `create/drop schema` cannot be implemented for Aerospike that requires static definition of namespaces using `aerospike.conf`.

### Identifiers
SQL identifier should follow the following rules: consist of Latin letters, digits and underscores (_) starting from a letter. If your identifier does not follow these rules wrap it with quotes. 

For example:
```sql
select name1 from data
```
Neither `name1` nor `data` should not be quoted. However:
```sql
select "first name" from "100"
```
Here `first name` is quoted because  it contains space while `100` is quoted because it starts with a digit. 

Even empty identifiers can be used:
```sql
insert into data_table (PK, "") values (1, 3.1415925)
select "" from data_table
select sin("") from data_table
```
Consequent double quotes in the example above indicate empty identifier. 

### Built-in Functions

| Function                         | Description                                                              |
| -------------------------------- | ------------------------------------------------------------------------ |
| `len(s)`, `length(s)`            | length of string, list, map                                              |
| `ascii(s)`                       | ASCII code of the first character of the given string                    |
| `char(code)`                     | character by its ASCII code                                              |
| `locate(subStr, str, [offset=1])`| returns position of subStr into str starting from offset (that is 1 if omitted)|
| `instr(subStr, str)`             | returns position of `subStr` into `str`                                  |
| `trim(s)`,`ltrim(s)`, `rtrim(s)` | trims string (removes spaces from both/left/right sides)                 |
| `strcmp(s1, s2)`                 | compares given strings                                                   |
| `left(str, n)`                   | n-characters substring of given string from the beginning                |
| `lower(str)`, lcase(str)         | converts all characters to lower case                                    |
| `upper(str)`, ucaes(str)         | converts all characters to upper case                                    |
| `str(v)`                         | converts given value to string. Implements special support for byte arrays.|
| `substring(str, start, length)`  | `length` characters long substring of given string started from `start`  |
| `space(n)`                       | generates string that consists of `n` spaces                             |
| `concat(...)`                    | concatenate given strings                                                |
| `concat(separator, ...)`         | concatenate given strings using separator                                |
| `reverse(str)`                   | reverses given string                                                    |
| `to_base64(bytes)`	           | generates Base64 representation of given byte array                      |
| `from_base64(str)`	           | returns byte array from given Base64                                     |
| `substr(str, from, to)`	       | returns substring of given string                                        |
| `concat(str1, str2, ...)`	       | concatenates given strings                                               |
| `concat_ws(separator, str1, str2, ...)`| concatenates given strings using separator                         |
| `now()`                          | retrieves current epoch time in milliseconds                             |
| `year([date])`                   | retrieves year (*)                                                       |
| `month([date])`                  | retrieves month (*)                                                      | 
| `dayofmonth([date])`             | retrieves day of month (*)                                               |
| `hour([date])`                   | retrieves hour (*)                                                       |
| `minute([date])`                 | retrieves minute (*)                                                     |
| `second([date])`                 | retrieves second (*)                                                     |
| `millisecond([date])`            | retrieves millisecond (*)                                                |
| `date([date])`                   | retrieves date object (*)                                                |
| `epoch(str, fmt)`                | parses given string representation of date using given format to epoch   |
| `map(s)`, `list(s)`, `array(s)`            | create map, list and array respectively from their string representation |
| `sin(x)`, `cos(x)`, `tan(x)`, `cot(x)`     | trigonometric functions                                        |
| `asin(x)`, `acos(x)`, `atan(x)`, `atan2(x)`| inverse trigonometric functions                                |
| `degrees(r)`, `radians(d)`                 | transforms radians to degrees and vice versa                   |                 
| `pi()`                                     | returns π                                                      | 
| `abs(x)`                                   | returns absolute value                                         |
| `floor(x)`, `ceil(x)`                      | return floor and ceil value of given `x`                       |
| `round(x, n)` | rounds given value `x` to scale `n`                                                         |
| `exp(x)`, `ln(x)`, `log10(x)`, `log2(x)`   | math functions e<sup>x</sup>, ln(x), lg<sub>10</sub>(x), lg<sub>2</sub>(x)|
| pow(x, n), power(x, n)                     | calculates x<sup>n</sup>                                       |



(*) `[date]` is optional argument. If omitted current date is used. Otherwise, can be either Date object or epoch or string representation of date parsed using one of the following formats: yyyy-MM-dd HH:mm:ss.SSS z, yyyy-MM-dd HH:mm:ss z, yyyy-MM-dd HH:mm:ss.SSS, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm, yyyy-MM-dd

## Working with serializable maps
Aerospike can hold map in single bin. One can either query map as-is or its fields separately. For example is the bin name is `data` and it contains map that hold personal data like `first_name`, `last_name` and `year_of_birth` the data can be queried as following:

```sql
select data[first_name], data[last_name], data[year_of_birth] from (select data from people)
```
Please note that Aerospike returns map of strings, so values of all fields including those that look like numeric are represented as strings. For example `year_of_birth` is not represented as number but as string that contains numeric characters.  

## Working with serializable classes
Let's take an example. We have class `Person`:

```java
public class Person implements Serializable {
    private String firstName;
    private String lastName;
    private int yearOfBirth;
    // getters, setters etc.
}
```
The first way to store objects of  this class in Aerospike is creating bins corresponding to the fields of the class, i.e. `firstName`, `lastName`, `yearOfBirth`. However, one can prefer to store the information in single bin. If class `Person` is serializable it can be easily done using `PreparedStatement`:

```java
PreparedStatement insert = conn.prepareStatement("insert into people (PK, data) values (?, ?)");
insert.setInt(1, 1);
insert.setObject(2, new Person("John", "Lennon", 1940));
```

Once this is done you can retrieve the information using select statement like:

```sql
select * data from people;
```

Unfortunately table `people` has only one column `data` that holds binary information. Fortunately serializable objects can be deserialized automatically using the following query:

```sql
select data[firstName], data[lastName], data[yearOfBirth] from (select data from people)
```
Where statement can be applied on both selects for example

```sql
select data[firstName], data[lastName], data[yearOfBirth] from (select data from people) where data[firstName]='John'
```

## Working with not serializable classes
Not serializable classes cannot be supported out-of-the-box. However, this problem can be solved using custom functions. 
Custom function is a public class that has public default constructor and implements `java.util.function.Function<T, R>` where `T` is a type of input parameter and `R` is a type of output parameter.

Let's take a look on example from the previous chapter but this time the class `Person` is not `Serializable`. Let's assume that data has been already written to the database, and we want to have a convenient way to retrieve it:

```sql
select human[firstName], human[lastName], human[yearOfBirth] from (select person(data) as human from people)
```

The statement calls function `person()` that converts data stored in the database to object of class `Person`. How is it possible? Indeed, driver does not know anything about either class custom `Person` or its serialization format. The answer is that `person()` is a custom function.

## Custom functions
The functionality of the driver can be extended by providing custom functions. Custom function is a public class that has public default constructor and implements `java.util.function.Function<T, R>` where `T` is a type of input parameter and `R` is a type of output parameter.
Here is an example of function that calculates square root of given numeric  parameter:

```java
public class Sqrt implements Function<Double, Double> {
    @Override
    public Double apply(Double d) {
        return Math.sqrt(d);
    }
}
```

This once implemented, compile and added to classpath this function must be registered as following:

```bash
jdbc:aerospike:localhost/test?custom.function.sqrt=com.company.Sqrt
```

Generally the parameter that registers custom function looks like:

```bash
custom.function.FUNCTION_NAME=FULLY_QUALIFIED_CLASS_NAME
```
Where
`FUNCTION_NAME` - the name that can be then used to call this function from SQL statement
`FULLY_QUALIFIED_CLASS_NAME` - the fully qualified class name of class that implements the function. One can define as many functions as he wants, e.g.:


```bash
jdbc:aerospike:localhost?custom.function.strlen=com.company.StrlengthCalculator&custom.function.sqrt=com.company.Sqrt
```

These functions can be used in SQL query as following:

```sql
select sqrt(4), strlen('abc'); -- returns 2, 3
select sqrt(strlen('abcd')); -- returns 2
```

Here is an example of custom function that deserializes custom binary representation of `Person` to instance of class `Person`:

```java
public class PersonDeserializer implements Function<byte[], Person> {
    @Override
    public Person apply(byte[] bytes) {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return new Person(dis.readUTF(), dis.readUTF(), dis.readInt());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
```
Customizations must be added to classpath of the application that uses driver. The custom function must be configured using configuration parameter supplied in JDBC URL:

```bash
jdbc:aerospike:localhost/test?custom.function.person=com.company.PersonDeserializer
```

This configuration makes function `person()` available for SQL queries.

## Truncate statement
The TRUNCATE TABLE command deletes the data inside a table.

The following SQL truncates the table "Categories": 

```sql
truncate table mytable
truncate table mynamespace.mytable
```
Additionally, to standard SQL syntax the Aerospike can truncate records older than specified in the special parameter "beforeLastUpdate":
```sql
truncate table mytable '2020-06-20'
truncate table mytable '2020-06-20 10:20:30.456'
```
The date should be specified using format like `yyyy-MM-dd[ HH:mm[:ss[.SSS[ z]]]]`


## Download
You can download binaries here:

*   [aerospike-jdbc-driver-1.0-SNAPSHOT.jar](https://drive.google.com/file/d/1BNpN_gA3E4C7CEZSLSuw1jOo2iNHtMBF/view?usp=sharing)
*   [aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar](https://drive.google.com/file/d/1ti9pQzJArutGnDnGCHMN1EFTMAG-rXyE/view?usp=sharing)

If you are running under Java 11 and higher you need Nashorn - java script engine that should be added to the classpath together with the driver. Take the Nashorn jar file [here](https://drive.google.com/file/d/1pb_5sxJbw-afxvJWNHuLf0ownvmM0Fjf/view?usp=sharing).

## Configuration of UI clients
Download [aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar](https://drive.google.com/file/d/1ti9pQzJArutGnDnGCHMN1EFTMAG-rXyE/view?usp=sharing) to your computer. This jar file contains the driver's binaries together with all dependencies. 

### Squirel SQL
#### Define driver
Copy `aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar` under `$SQUIREL_SQL_HOME/drivers`.
Open Squirel SQL application, choose drivers tab at the left pane, press "add" button and fill form:
*   Name: Aerospike
*   Example URL: `jdbc:aerospike:host:3000/test`
*   Class name: `com.nosqldriver.aerospike.sql.AerospikeDriver`
Add the path to jar file to "Extra class path" panel. 

#### Create DB connection
Choose Aliases tab at the left pane and press add button. 
Fill form: name, choose the driver from list, URL, username and password (if needed)

### DBeaver
#### Define driver
Choose Database/DriverManager, then press button "New."
Fill the following data:
*   Driver name: Aerospike
*   Class name: `com.nosqldriver.aerospike.sql.AerospikeDriver`
*   URL template: `jdbc:aerospike:{host}[:{port}]/[{database}]`
*   Default port: 3000

#### Create DB connection
*   Choose Database/New Database Connection
*   Find just defined Aerospike driver in list and choose it.
*   Fill form: host, port (if it is not default), Database/Schema (namespace) (if needed); username and password (if needed)
