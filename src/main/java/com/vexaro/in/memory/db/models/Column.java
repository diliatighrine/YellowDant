package com.vexaro.in.memory.db.models;

import lombok.Data;

@Data
public class Column {
    private String name;
    private String type;


    private boolean indexed; // Ajouter un indicateur pour savoir si la colonne est indexée

    // Constructeur
    public Column(String name, String type, boolean indexed) {
        this.name = name;
        this.type = type;
        this.indexed = indexed;
    }
}
