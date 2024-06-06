package com.vexaro.input;

import java.util.List;

public class Select {

    private String from;
    private List<ColumnInput> columns;
    private List<Where> where;
    private String groupBy;


    // Getters and Setters
    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public List<ColumnInput> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnInput> columns) {
        this.columns = columns;
    }

    public List<Where> getWhere() {
        return where;
    }

    public void setWhere(List<Where> where) {
        this.where = where;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }



    
}
