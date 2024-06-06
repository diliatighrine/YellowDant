package com.vexaro.in.memory.db.services;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

@Component
public class NodeDiscovery {

   private Map<String, List<String>> tableNodeMap; // Mapping des tables à leurs nœuds

   public NodeDiscovery() {
       tableNodeMap = new HashMap<>();
   }

   public List<String> discoverActiveNodes() {
       List<String> activeNodes = new ArrayList<>();
       int timeout = 1000; // Timeout en millisecondes

       // Définir une plage d'adresses IP à vérifier
       String baseIpAddress = "192.168.1.";
       int startRange = 1;
       int endRange = 10;

       for (int i = startRange; i <= endRange; i++) {
           String ipAddress = baseIpAddress + i;
           try {
               InetAddress inet = InetAddress.getByName(ipAddress);
               if (inet.isReachable(timeout)) {
                   activeNodes.add(ipAddress);
               }
           } catch (IOException e) {
               // Gérer les exceptions
           }
       }

       return activeNodes;
   }

   public void mapTableToNode(String tableName, String nodeAddress) {
       if (!tableNodeMap.containsKey(tableName)) {
           tableNodeMap.put(tableName, new ArrayList<>());
       }
       tableNodeMap.get(tableName).add(nodeAddress);
   }

   public List<String> getNodesWithTable(String tableName) {
       return tableNodeMap.getOrDefault(tableName, new ArrayList<>());
   }

   public static void main(String[] args) {
       NodeDiscovery nodeDiscovery = new NodeDiscovery();
       List<String> activeNodes = nodeDiscovery.discoverActiveNodes();
       System.out.println("Active nodes: " + activeNodes);
   }
}
