package com.nosqldriver.sql;

public class OrderItem {
    private final String name;
    private final Direction direction;


    public OrderItem(String name) {
        this(name, Direction.ASC);
    }

    public OrderItem(String name, Direction direction) {
        this.name = name;
        this.direction = direction;
    }

    public String getName() {
        return name;
    }

    public Direction getDirection() {
        return direction;
    }

    public enum Direction {
        ASC, DESC
    }
}
