package com.vexaro.in.memory.db.services;
import java.util.List;
import java.util.Random;
import org.springframework.stereotype.Component;
import com.vexaro.in.memory.db.models.Partition;

@Component
public class LoadBalancer {

    public String selectNode(List<String> nodes) {
        Random random = new Random();
        int index = random.nextInt(nodes.size());
        return nodes.get(index);
    }
    public void balanceLoad(List<String> nodes, List<Partition> partitions) {
        int totalNodes = nodes.size();
        int totalPartitions = partitions.size();
       
        if (totalNodes == 0 || totalPartitions == 0) {
            return; // Rien à équilibrer
        }
       
        int partitionsPerNode = totalPartitions / totalNodes;
        int remainingPartitions = totalPartitions % totalNodes;
        int assignedPartitions = 0;
       
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

