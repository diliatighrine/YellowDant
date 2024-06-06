package com.vexaro.input;

public class ColumnInput {

    private String aggregate;
    private String column;


    // Constructor
    public ColumnInput(String column) {
        this.column = column;
    }

    // Default constructor
    public ColumnInput() {
    }

    // Getters and Setters
    public String getAggregate() {
        return aggregate;
    }

    public void setAggregate(String aggregate) {
        this.aggregate = aggregate;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    @Override
    public String toString() {
        return "ColumnInput{" +
                "column='" + column + '\'' +
                ", aggregate='" + aggregate + '\'' +
                '}';
    }
    
}
