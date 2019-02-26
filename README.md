# BeanLane [![CircleCI](https://circleci.com/gh/alexradzin/beanlane/tree/master.svg?style=svg)](https://circleci.com/gh/alexradzin/beanlane/tree/master)

A simple utility that allows getting the name of bean properties as string without reflection.


## Motivation

There are a lot of libraries that operate with string representation of bean properties usually when building the
query criteria. For example Hibernate, JPA, MongoDB client etc. The following example shows query of Mongo DB:

```java
// connection
Datastore datastore = new Morphia().createDatastore(new MongoClient(), "people");
// query
Query<Person> query = datastore.createQuery(Person.class);
query.and(
        query.criteria("firstName").equalIgnoreCase(firstName),
        query.criteria("lastName").equalIgnoreCase(lastName),
        query.criteria("age").greaterThanOrEq(age));
query.order(Sort.descending("lastName")).asList(new FindOptions().skip(limit * page).limit(limit));
```

Existence of strings "firstName", "lastName" and "age" here is bad. If the appropriate fields in model are renamed to
"givenName" and "familyName" one have to go through all queries and fix them. In case of Hibernate the wrong queries will
at least throw exception at runtime, that help to locate problems earlier. But schema-less Mongo queries will just return
wrong results whithout any failures.

Libraries like jOOQ and QueryDSL solve this problems using code generation. BeanLane suggests solution without any code generation,
so you can continue using your favorite criteria API in slightly safer manner.

```java
// query
Query<Person> query = datastore.createQuery(Person.class);
Person p = $(Person.class);
query.and(
        query.criteria($(p::getFirstName)).equalIgnoreCase(firstName),
        query.criteria($(p::getLastName)).equalIgnoreCase(lastName),
        query.criteria($(p::getAge)).greaterThanOrEq(age));
query.order(Sort.descending("lastName")).asList(new FindOptions().skip(limit * page).limit(limit));
```

As we can see in this example "magic" function call `$(p::getFirstName)` returns string `firstName`, `$(p::getAge)` returns `lastName` etc. 


## Quick start

The library is still not published in maven repository but this will be done soon. Onece this is done just include its artifact into your dependency management script, e.g.

```java
compile 'com.github:org.beanlane:1.0.0'
```

The simplest way to use the library is to make your DAO layer class to implement `BeanLaneSpec`, i.e.

```java
public class MyDao implements BeanLaneSpec {
}
```

Once this is done 3 magic functions become available:
*  `$()` that generates bean property names (e.g. `lastName`, `age`, `home.street`)
* `__()` that generates snake lower case names (e.g. `last_name`)
* `___()` that generates snake upper case names (e.g. `LAST_NAME`)



## How does it work?

The interface `BeanLaneSpec` provides several sort named default functions that delegate implementation into to
class `BeanLane`. `BeanLane` uses CGLIB to generate proxy over provided class (`Person` in our example), so the names of called 
methods become avaliable and can be returned to application level code. 

Class `BeanLane` has several configuration parameters they can be used if you instantiate it directly without interface
`BeanLaneSpec`. This also allows you to change names of magic function according to your taste.


## Why names of magic functions do not follow naming conventions?

Well, each function with short name has synonym with conventional name. However, IMHO short names just improve readability.
The statement `$(p::getFirstName)` just a little bit longer than `"firstName"`, however `getName(p::getFirstName)`
is significantly longer and not clearer.


## Extracting field names from annotations

Sometimes we want to get name of field from annotation exactly as verious O[R]M frameworks do. BeanLane has generic container
annotation that can be used to configure the library to use other annotation. Just make your DAO to implmenent `BeanLaneAnnotationSpec`
and mark it with annotation `@BeanNameAnnotation`:

```java
@BeanNameAnnotation(value = XmlElement.class, field = "name")
public class MyXmlDao implements BeanNameAnnotationSpec {
}

@BeanNameAnnotation(value = JsonProperty.class)
public class MyJsonDao implements BeanNameAnnotationSpec {
}
```

`BeanNameAnnotationSpec` uses strings extracted from annotations instead of from class fields. For example if classPerson` is defined as following:

```
public class Person {
    @XmlElement(name = "FirstName") private String firstName;
    @XmlElement(name = "LastName") private String lastName;
    @XmlElement(name = "HomeAddress") private Address home;
    //..........
}
```

We can use it as following:

```
@BeanNameAnnotation(value = XmlElement.class, field = "name")
class PersonDao implements BeanLaneAnnotationSpec {
    public void foo() {
        Person p = $(Person.class);
        $(p::getFirstName);                     // returns FirstName
        $(() -> p.getHome().getStreetNumber()); // returns omeAddress.StreetNumber
    }
}
```




