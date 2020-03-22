# Aerospike JDBC Driver 

[![Build Status](https://travis-ci.com/alexradzin/aerospike-jdbc-driver.svg?branch=master)](https://travis-ci.com/alexradzin/aerospike-jdbc-driver)
[![codecov](https://codecov.io/gh/alexradzin/aerospike-jdbc-driver/branch/master/graph/badge.svg)](https://codecov.io/gh/alexradzin/aerospike-jdbc-driver)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/63282ca8b0ff451ba30fea499b32408f)](https://www.codacy.com/app/alexradzin/aerospike-jdbc-driver?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=alexradzin/aerospike-jdbc-driver&amp;utm_campaign=Badge_Grade)

## Motivation

Simplify access to Aerospike DB.

## Introduction

[Aerospike](https://www.aerospike.com/) is extremely fast and scalable key-value store. It provides clients for various languages including Java. However unlike relational databases that all support SQL and therefore can easily implement JDBC driver each no-SQL DB creates its own world. API exposed by clients of no-SQL databases are different (not standard) and optimized for specific features of certain databases. 

From other hand threre are a lot of tools that help to visualize data stored in database, create reports based on the data, perform various ETL operations etc. These tools traditionally support SQL. Database that has standards complient JDBC driver can be easilty connected to various tools. Majority of popular no-SQL databased have JDBC drivers. Aerospike did not have one. This was the reason to start this project. 

## Presentation

Slides for presentation can be found [here.](https://docs.google.com/presentation/d/1McxNantOIO51rEwuh-ns_PLupZkAGJjtsS9SDhkpboc/edit?usp=sharing)

## Key Features
*   Use either a single all-in-one JAR or define dependency (TBD)
*   Use standard SQL to insert, update, retrieve and delete data to/from Aerospike DB
*   Compliant with JDBC JDBC 4.0.
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

### JDBC URL

The JDBC URL format is: `jdbc:aerospike:HOST[:PORT][/NAMESPACE][?PARAM1=VALUE1[&PARAM2=VALUE2]`.
For example `jdbc:aerospike:localhost` connectts to aerospike running on local machine and listening to the default port (3000). If you want to customize port use URL like `jdbc:aerospike:localhost:4000`, to connecto to specific namespace add it to URL like: `jdbc:aerospike:localhost/test`. The following example shows how to connect to namespace `test` of aerospike running on remote machine and listening to port 4567: `jdbc:aerospike:10.1.1.1:4567/test`.

Typical Aerospike installation consists of several instances, so several IP addresses of servers can be passed to driver as following: `jdbc:aerospike:first:3100,second:3200,third:3300`. If port is omitted, the default value of 3000 is used. 

## SQL compliance

### Supported statements
*   insert, update, delete, select
*   Complex where clause can be used with update, delete and select. Use special field "PK" to refere to the primary key. Syntanctically there is no difference between referencing to primary key or any data column. Select statement does its best effort to use secondary indexes if defined. 
*   Nested selects are supported. 
*   Select can be used with distinct, group by, order by.
*   create/drop index
*   use `namespace_name` to change active namespace. Useful for interactive mode or scripts. 

### Statements that will be supported in future
*   drop table
*   describe
*   show schemas/tables/indexes

### Statements that will not be supported
*   create table. This operation is meaningless applicable to Aerospike that creates set once somebody writes to this set. 
*   create/drop schema just cannot be implemented for Aerospike that requires static definition of namespaces using `aerospike.conf`.

### Built-in Functions

| Function                         | Description                                                              |
| -------------------------------- | ------------------------------------------------------------------------ |
| `len(s)`                         | length of string                                                         |
| `ascii(s)`                       | ASCII code of the first character of the given string                    |
| `char(code)`                     | character by its ASCII code                                              |
| `charIndex(subStr, str, start)`  | starting position of `subStr` into `str` starting from `start` index     |
| `left(str, n)`                   | n-characters sustring of given string from the beginning                 |
| `lower(str, n)`                  | converts all characters to loewer case                                   |
| `upper(str, n)`                  | converts all characters to upper case                                    |
| `str(n)`                         | converts given number to string                                          |
| `substring(str, start, length)`  | `length` characters long substring of given string started from `start`  |
| `space(n)`                       | generates string that consists of `n` spaces                             |
| `concat(...)`                    | concatenate given strings                                                |
| `concat(separator, ...)`         | concatenate given strings using separator                                |
| `reverse(str)`                   | reverses given string                                                    |
| `now()`                          | retrieves current epoch time in milliseconds                             |
| `year([date])`                   | retrueves year (*)                                                       |
| `month([date])`                  | retrueves month (*)                                                      | 
| `dayofmonth([date])`             | retrueves day of month (*)                                               |
| `hour([date])`                   | retrueves hour (*)                                                       |
| `minute([date])`                 | retrueves minute (*)                                                     |
| `second([date])`                 | retrueves second (*)                                                     |
| `millisecond([date])`            | retrueves milliesecond (*)                                               |
| `date([date])`                   | retrueves date object (*)                                                |
| `epoch(str, fmt)`                | parses given string representation of date using given format to epoch   |

(*) `[date]` is optional argument. If omitted current date is used. Otherwise can be either Date object or epoch or string representation of date parsed using one of the following formats: yyyy-MM-dd HH:mm:ss.SSS z, yyyy-MM-dd HH:mm:ss z, yyyy-MM-dd HH:mm:ss.SSS, yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm, yyyy-MM-dd

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
The first way to store objects of  this class in Aerospike is creating bins corresponding to the fields of the class, i.e. `firstName`, `lastName`, `yearOfBirth`. However one can prefer to store the information in single bin. If class `Person` is serializable it can be easily done using `PreparedStatement`:

```java
PreparedStatement insert = conn.prepareStatement("insert into people (PK, data) values (?, ?)");
insert.setInt(1, 1);
insert.setObject(2, new Person("John", "Lennon", 1940));
```

Onece this is done you can retrieve the information using select statement like:

```sql
select * data from people;
```

Unfortunately table `people` has onlyy one column `data` that holds binary information. Fortunately serializable objects can be deserialized automatically using the following query:

```sql
select data[firstName], data[lastName], data[yearOfBirth] from (select data from people)
```
Where statement can be applied on both selects for example

```sql
select data[firstName], data[lastName], data[yearOfBirth] from (select data from people) where data[firstName]='John'
```

## Working with not serializable classes
Not serializable classes cannot be supported out-of-the-box. However this problem can be solved using custom functions. 
Custom function is a public class that has public default constructor and implements `java.util.function.Function<T, R>` where `T` is a type of input parameter and `R` is a type of output parameter.

Let's take a look on example from the previous chapter but this time the class `Person` is not `Serializable`. Let's assume that data has been already written to the database and we want to have a convenient way to retrieve it:

```sql
select data[firstName], data[lastName], data[yearOfBirth] from (select person(data) from people)
```

The statement calls function `person()` that converts data stored in database to object of class `Person`. But how is it possible? Indeed driver does not know anything about either class custom `Person` or its serialization format. The answer is that `person()` is a custom function.

## Custom functions
The functionality of the driver can be extended by providing custom functions. Custom function is a public class that has public default constructor and implements `java.util.function.Function<T, R>` where `T` is a type of input parameter and `R` is a type of output parameter.
Here is an example of function that calculates squre root of given numeric  parameter:

```java
public class Sqrt implements Function<Double, Double> {
    @Override
    public Double apply(Double d) {
        return Math.sqrt(d);
    }
}
```

This once implemented, compile and added to classpath this function must be registered as following:

```
jdbc:aerospike:localhost/test?custom.function.sqrt=com.company.Sqrt
```

Generally the parameter that registers custom function looks like:

```
custom.function.FUNCTION_NAME=FULLY_QUALIFIED_CLASS_NAME
```
Where
`FUNCTION_NAME` - the name that can be then used to call this function from SQL statement
`FULLY_QUALIFIED_CLASS_NAME` - the fully qualified class name of class that implements the function. One can define as many functions as he wants, e.g.:


```
jdbc:aerospike:localhost?custom.function.strlen=com.company.StrlengthCalculator&custom.function.sqrt=com.company.Sqrt
```

These functions can be used in SQL query as following:

```sql
select sqrt(4), strlen('abc'); -- returns 2, 3
select sqrt(strlen('abcd')); -- returns 2
```

Here is an example of custom function that deserializes custom binary representation of `Persoon` to instance of class `Person`:

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

```
jdbc:aerospike:localhost/test?custom.function.person=com.company.PersonDeserializer
```

This configuration makes function `person()` available for SQL queries.


## Download
You can download binaries here:

* [aerospike-jdbc-driver-1.0-SNAPSHOT.jar](https://drive.google.com/file/d/1BNpN_gA3E4C7CEZSLSuw1jOo2iNHtMBF/view?usp=sharing)
* [aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar](https://drive.google.com/file/d/1ti9pQzJArutGnDnGCHMN1EFTMAG-rXyE/view?usp=sharing)

If you are running under Java 11 and highier you need nashorn - java script engine that should be added to the classpath together with the driver. Take the nashorn jar file [here](https://drive.google.com/file/d/1pb_5sxJbw-afxvJWNHuLf0ownvmM0Fjf/view?usp=sharing).


## Configuration of UI clients
Download [aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar](https://drive.google.com/file/d/1ti9pQzJArutGnDnGCHMN1EFTMAG-rXyE/view?usp=sharing) to your computer. This jar file contains the driver's binaris together with all dependenciees. 

### Squirel SQL
#### Define driver
Copy `aerospike-jdbc-driver-all-1.0-SNAPSHOT.jar` under `$SQUIREL_SQL_HOME/drivers`.
Open Squirel SQL application, choose drivers tab at the left pane, press "add" button and fill form:
* Name: Aerospike
* Example URL: `jdbc:aerospike:host:3000/test`
* Class name: `com.nosqldriver.aerospike.sql.AerospikeDriver`
Add the path to jar file to "Extra class path" panel. 

#### Create DB connection
Choose Aliases tab at the left pane and press add button. 
Fill form: name, choose driver from list, URL, username and password (if needed)



### DBeaver
#### Define driver
Choose Database/DriverManager, then press button "New."
Fill the following data
* Driver name: Aerospike
* Class name: `com.nosqldriver.aerospike.sql.AerospikeDriver`
* URL template: `jdbc:aerospike:{host}[:{port}]/[{database}]`
* Default port: 3000

#### Create DB connection
* Choose Database/New Database Connection
* Find just defined Aerospike driver in list and choose it. 
* Fill form: host, port (if it is not default), Database/Schema (namespace) (if needed); username and password (if needed)




