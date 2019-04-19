package com.nosqldriver;

public class Person {
    private final int id;
    private final String firstName;
    private final String lastName;
    private final int yearOfBirth;
    private final int kidsCount;

    public Person(int id, String firstName, String lastName, int yearOfBirth, int numberOfChildren) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.yearOfBirth = yearOfBirth;
        this.kidsCount = numberOfChildren;
    }

    public int getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getYearOfBirth() {
        return yearOfBirth;
    }

    public int getKidsCount() {
        return kidsCount;
    }
}
