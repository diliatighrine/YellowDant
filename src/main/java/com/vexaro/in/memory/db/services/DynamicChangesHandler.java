package com.vexaro.in.memory.db.services;


import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.vexaro.in.memory.db.models.Partition;

@Component
public class DynamicChangesHandler {
    public void handleNodeAdded(String newNode, List<Partition> partitions) {
        // Ajouter le nouveau nœud à la liste des nœuds existants
        List<String> existingNodes = extractNodes(partitions);
        existingNodes.add(newNode);
        // Redistribution des partitions
        redistributePartitions(partitions, existingNodes);
    }

    public void handleNodeRemoved(String removedNode, List<Partition> partitions) {
        // Supprimer le nœud supprimé de la liste des nœuds existants
        List<String> existingNodes = extractNodes(partitions);
        existingNodes.remove(removedNode);
        // Redistribution des partitions
        redistributePartitions(partitions, existingNodes);
    }

    private List<String> extractNodes(List<Partition> partitions) {
        // Extraire tous les nœuds existants à partir des partitions
        List<String> nodes = new ArrayList<>();
        for (Partition partition : partitions) {
            String node = partition.getNode();
            if (!nodes.contains(node)) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    private void redistributePartitions(List<Partition> partitions, List<String> nodes) {
        // Nombre total de nœuds et de partitions
        int totalNodes = nodes.size();
        int totalPartitions = partitions.size();

        if (totalNodes == 0 || totalPartitions == 0) {
            return; // Rien à redistribuer
        }

        // Nombre de partitions par nœud
        int partitionsPerNode = totalPartitions / totalNodes;
        int remainingPartitions = totalPartitions % totalNodes;
        int assignedPartitions = 0;

        // Répartition des partitions entre les nœuds
        for (int i = 0; i < totalNodes; i++) {
            int partitionsForThisNode = partitionsPerNode;
            if (i < remainingPartitions) {
                partitionsForThisNode++;
            }
            for (int j = 0; j < partitionsForThisNode; j++) {
                partitions.get(assignedPartitions++).setNode(nodes.get(i));
            }
        }
    }
}

