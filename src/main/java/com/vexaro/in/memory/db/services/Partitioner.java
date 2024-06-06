package com.vexaro.in.memory.db.services;

import java.util.List;

import org.springframework.stereotype.Component;

import com.vexaro.in.memory.db.services.NodeDiscovery;

@Component
public class Partitioner {
    private NodeDiscovery nodeDiscovery;

    public Partitioner(NodeDiscovery nodeDiscovery) {
        this.nodeDiscovery = nodeDiscovery;
    }

    public String getNodeForKey(String key) {
        List<String> nodes = nodeDiscovery.discoverActiveNodes();
        // Implémentez votre logique de hachage ici pour mapper la clé au nœud responsable
        int hash = key.hashCode() % nodes.size();
        return nodes.get(hash);
    }
}

