package com.vexaro.in.memory.db.models;


public class Partition {
    private String node; // Le nœud responsable de cette partition

    // Constructeur
    public Partition(String node) {
        this.node = node;
    }

    // Getter et setter pour le nœud responsable
    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }
}

