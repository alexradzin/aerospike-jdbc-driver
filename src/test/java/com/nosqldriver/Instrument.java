package com.nosqldriver;

public class Instrument {
    private final int id;
    private final int personId;
    private final String name;

    public Instrument(int id, int personId, String name) {
        this.id = id;
        this.personId = personId;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public int getPersonId() {
        return personId;
    }

    public String getName() {
        return name;
    }
}
