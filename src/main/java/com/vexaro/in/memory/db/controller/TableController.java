
package com.vexaro.in.memory.db.controller;

import com.vexaro.in.memory.db.dto.PaginatedTableData;
import com.vexaro.in.memory.db.dto.TableRowsRequest;
import com.vexaro.in.memory.db.models.Column;
import com.vexaro.in.memory.db.models.TableDefinition;
import com.vexaro.in.memory.db.services.DynamicChangesHandler;
import com.vexaro.in.memory.db.services.LoadBalancer;
import com.vexaro.in.memory.db.services.NodeDiscovery;
import com.vexaro.in.memory.db.services.ParquetDataService;
import com.vexaro.in.memory.db.services.Partitioner;
import com.vexaro.in.memory.db.services.TableService;
import com.vexaro.input.ColumnInput;
import com.vexaro.input.Select;
import com.vexaro.input.Where;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import org.eclipse.jetty.util.Callback.Completable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.springframework.web.client.RestTemplate;

@Slf4j
@RestController
@RequestMapping("/api/tables")
@RequiredArgsConstructor
public class TableController {

    private final TableService tableService;
    private final ParquetDataService parquetDataService;
    private final NodeDiscovery nodeDiscovery;
    private final Partitioner partitioner;
    private final LoadBalancer loadBalancer;
    private final DynamicChangesHandler dynamicChangesHandler;

    @Autowired
    private RestTemplate restTemplate;

    @PostMapping("/create")
    ResponseEntity<String> createTableSchema(@RequestBody TableDefinition tableDefinition) {
        tableService.createSchema(tableDefinition);
        return ResponseEntity.ok("Table created Successfully");
    }

    @PostMapping("/load/{tableName}")
    ResponseEntity<String> loadTableData(@PathVariable String tableName, @RequestBody TableRowsRequest data) {
        String responsibleNode = partitioner.getNodeForKey(tableName);

        // Préparer l'URL du nœud responsable
        String url = "http://" + responsibleNode + "/api/tables/load/" + tableName;

        // Préparer les en-têtes et le corps de la requête
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<TableRowsRequest> requestEntity = new HttpEntity<>(data, headers);

        // Envoyer la requête au nœud responsable
        restTemplate = new RestTemplate();
        ResponseEntity<String> responseEntity = restTemplate.exchange(url, HttpMethod.POST, requestEntity,
                String.class);

        // Vérifier la réponse
        if (responseEntity.getStatusCode() == HttpStatus.OK) {
            return ResponseEntity.ok("Data loaded Successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Failed to load data in '" + tableName + "' schema on node: " + responsibleNode);
        }
    }

    @GetMapping("/retrieve/{tableName}")
    ResponseEntity<List<List<String>>> retrieveTableData(@PathVariable String tableName) {
        List<String> activeNodes = nodeDiscovery.discoverActiveNodes();
        if (activeNodes.isEmpty()) {
            // Aucun nœud disponible
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        // Utiliser LoadBalancer pour équilibrer la charge entre les nœuds disponibles
        String selectedNode = loadBalancer.selectNode(activeNodes);

        // Envoyer une requête pour récupérer les données au nœud sélectionné
        ResponseEntity<List<List<String>>> response = sendRetrieveRequest(selectedNode, tableName);
        if (response.getStatusCode().is2xxSuccessful()) {
            return ResponseEntity.ok(response.getBody());
        } else {
            // Gérer les erreurs de récupération des données depuis le nœud sélectionné
            return ResponseEntity.status(response.getStatusCode()).build();
        }
    }

    private ResponseEntity<List<List<String>>> sendRetrieveRequest(String node, String tableName) {
        String url = "http://" + node + "/api/tables/" + tableName;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);

        return restTemplate.exchange(url, HttpMethod.GET, entity, new ParameterizedTypeReference<List<List<String>>>() {
        });
    }

    @GetMapping("/{tableName}")
    ResponseEntity<PaginatedTableData> getTableData(
            @PathVariable String tableName,
            @RequestParam(required = false, defaultValue = "0") int page,
            @RequestParam(required = false, defaultValue = "10") int size) {

        int offset = page * size;
        PaginatedTableData paginatedTableData = tableService.retrieveTableDataWithPagination(tableName, offset, size);
        if (paginatedTableData != null && paginatedTableData.getRows() != null) {
            return ResponseEntity.ok(paginatedTableData);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/loadParquet/{tableName}")
    public ResponseEntity<String> loadParquetData(@PathVariable String tableName,
            @RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("File is empty");
        }

        // Write the file to a local temp file
        File tempFile;
        try {
            tempFile = File.createTempFile("parquet-upload-", ".parquet");
            file.transferTo(tempFile);
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error writing uploaded file to disk");
        }

        try {
            // Process the file after it's been written to disk
            parquetDataService.parseParquetFile(tempFile.getAbsolutePath(), rows -> {
                // charger les données de maniere asynchrone
                CompletableFuture.runAsync(() -> tableService.loadTableData(tableName, rows));
            });

            return ResponseEntity.ok("Data loaded successfully into table: " + tableName);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error loading Parquet data: " + e.getMessage());
        } finally {
            // Make sure to clean up the temp file
            if (tempFile != null && tempFile.exists()) {
                tempFile.delete();
            }
        }
    }

    /*
     * @GetMapping("/{tableName}/aggregate")
     * public ResponseEntity<Map<String, Object>> performAggregate(
     * 
     * @PathVariable String tableName,
     * 
     * @RequestParam String function,
     * 
     * @RequestParam String column) {
     * double result = tableService.aggregate(tableName, function, column);
     * return ResponseEntity.ok(Collections.singletonMap("result", result));
     * }
     */
    @GetMapping("/{tableName}/aggregate")
    public ResponseEntity<Map<String, Object>> performAggregate(
            @PathVariable String tableName,
            @RequestParam String function,
            @RequestParam String column) {
        List<String> activeNodes = nodeDiscovery.discoverActiveNodes();
        if (activeNodes.isEmpty()) {
            // Aucun nœud disponible
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }

        // Utiliser LoadBalancer pour équilibrer la charge entre les nœuds disponibles
        List<String> nodesWithTable = nodeDiscovery.getNodesWithTable(tableName);
        if (nodesWithTable.isEmpty()) {
            // Aucun nœud avec la table spécifiée
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Collections.singletonMap("error", "Table '" + tableName + "' not found on any node"));
        }

        String selectedNode = loadBalancer.selectNode(nodesWithTable);

        // Envoyer une requête pour effectuer l'agrégation sur le nœud sélectionné
        ResponseEntity<Map<String, Object>> response = sendAggregateRequest(selectedNode, tableName, function, column);
        if (response.getStatusCode().is2xxSuccessful()) {
            return ResponseEntity.ok(response.getBody());
        } else {
            // Gérer les erreurs de récupération des données depuis le nœud sélectionné
            return ResponseEntity.status(response.getStatusCode())
                    .body(Collections.singletonMap("error", "Failed to perform aggregation on node: " + selectedNode));
        }
    }

    private ResponseEntity<Map<String, Object>> sendAggregateRequest(String node, String tableName, String function,
            String column) {
        String url = "http://" + node + "/api/tables/" + tableName + "/aggregate?function=" + function + "&column="
                + column;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(headers);

        return restTemplate.exchange(url, HttpMethod.GET, entity,
                new ParameterizedTypeReference<Map<String, Object>>() {
                });
    }

    // Endpoint pour la communication entre les nœuds
    @PostMapping("/communication")
    public ResponseEntity<String> nodeCommunication(@RequestBody String message) {
        log.info("Received message from another node: {}", message);
        // Analyser le message et effectuer les actions nécessaires (insertion, mise à
        // jour, suppression)
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(message);
            String operationType = jsonNode.get("operation_type").asText();
            String tableName = jsonNode.get("table_name").asText();
            JsonNode dataNode = jsonNode.get("data");
            switch (operationType) {
                case "insert":
                    List<String> insertData = parseDataNode(dataNode);
                    tableService.insertData(tableName, insertData);
                    break;
                case "update":
                    int rowIndex = jsonNode.get("row_index").asInt();
                    List<String> updatedData = parseDataNode(dataNode);
                    tableService.updateData(tableName, rowIndex, updatedData);
                    break;
                case "delete":
                    int deleteIndex = jsonNode.get("row_index").asInt();
                    tableService.deleteData(tableName, deleteIndex);
                    break;
                default:
                    log.error("Unsupported operation type: {}", operationType);
                    return ResponseEntity.badRequest().body("Unsupported operation type: " + operationType);
            }
            return ResponseEntity.ok("Message received and processed successfully");
        } catch (IOException e) {
            log.error("Error while processing received message: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing message");
        }
    }

    // Méthode pour synchroniser les données avec d'autres nœuds private void
    // synchronizeData(String tableName, JsonNode data, String operationType) {
    // log.info("Synchronizing data for table '{}'", tableName); List<String>
    // nodeAddresses = nodeDiscovery.discoverActiveNodes(); for (String nodeAddress
    // : nodeAddresses) { try { HttpClient client = HttpClient.newHttpClient();
    // ObjectMapper objectMapper = new ObjectMapper(); ObjectNode message =
    // objectMapper.createObjectNode(); message.put("operation_type",
    // operationType); message.put("table_name", tableName); message.set("data",
    // data); HttpRequest request = HttpRequest.newBuilder()
    // .uri(URI.create("http://" + nodeAddress + "/api/tables/communication"))
    // .header("Content-Type", "application/json")
    // .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(message)))
    // .build(); HttpResponse<String> response = client.send(request,
    // HttpResponse.BodyHandlers.ofString()); if (response.statusCode() == 200) {
    // log.info("Data synchronized successfully with node: {}", nodeAddress); } else
    // { log.error("Failed to synchronize data with node: {}", nodeAddress); } }
    // catch (IOException | InterruptedException e) { log.error("Error while
    // synchronizing data with node: {}", nodeAddress); e.printStackTrace(); } } }
    // // Méthode pour envoyer une demande de synchronisation à un nœud spécifique
    // private void sendSynchronizationRequest(String nodeAddress, String tableName,
    // JsonNode data, String operationType) { try { HttpClient client =
    // HttpClient.newHttpClient(); ObjectMapper objectMapper = new ObjectMapper();
    // ObjectNode message = objectMapper.createObjectNode();
    // message.put("operation_type", operationType); message.put("table_name",
    // tableName); message.set("data", data); HttpRequest request =
    // HttpRequest.newBuilder() .uri(URI.create("http://" + nodeAddress +
    // "/api/tables/communication")) .header("Content-Type", "application/json")
    // .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(message)))
    // .build(); HttpResponse<String> response = client.send(request,
    // HttpResponse.BodyHandlers.ofString()); if (response.statusCode() == 200) {
    // log.info("Synchronization request sent to node '{}' for table '{}'",
    // nodeAddress, tableName); } else { log.error("Failed to send synchronization
    // request to node: {}", nodeAddress); } } catch (IOException |
    // InterruptedException e) { log.error("Error while sending synchronization
    // request to node: {}", nodeAddress); e.printStackTrace(); } }

    @PostMapping("/synchronize")
    public ResponseEntity<String> synchronizeDataFromNode(@RequestBody String synchronizationData) {
        try {
            // Convertir les données de synchronisation JSON en List<List<String>>
            ObjectMapper objectMapper = new ObjectMapper();
            List<List<String>> data = objectMapper.readValue(synchronizationData, new TypeReference<List<List<String>>>() {});

            // Traiter les données de synchronisation et mettre à jour la table locale
            for (List<String> row : data) {
                String tableName = row.get(0);
                row.remove(0); // Supprimer le nom de la table

                // Convertir List<String> en Map<Integer, Map<String, String>>
                Map<Integer, Map<String, String>> tableData = new HashMap<>();
                for (int i = 0; i < row.size(); i++) {
                    Map<String, String> rowData = new HashMap<>();
                    String[] columns = row.get(i).split(",");
                    for (String column : columns) {
                        String[] keyValue = column.split(":");
                        if (keyValue.length == 2) {
                            rowData.put(keyValue[0], keyValue[1]);
                        }
                    }
                    tableData.put(i, rowData);
                }

                tableService.loadTableData(tableName, tableData);
            }

            // Répondre avec un message de confirmation ou d'erreur
            return ResponseEntity.ok("Synchronization data processed successfully");
        } catch (IOException e) {
            // Gérer les erreurs de désérialisation des données JSON
            log.error("Error processing synchronization data", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing synchronization data: " + e.getMessage());
        }
    }

    // select all
    @GetMapping("/retrieveAll/{tableName}")
    ResponseEntity<List<List<String>>> retrieveAllTableData(@PathVariable String tableName) {
        TableDefinition tableDefinition = tableService.retrieveTableData(tableName);
        if (tableDefinition != null && tableDefinition.getRows() != null) {
            return ResponseEntity.ok(tableDefinition.getRowsAsList());
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    // select where
    @GetMapping("/retrieveByCondition/{tableName}")
    ResponseEntity<List<List<String>>> retrieveTableDataByCondition(
            @PathVariable String tableName,
            @RequestParam String column,
            @RequestParam String value) {
        TableDefinition tableDefinition = tableService.retrieveTableData(tableName);
        if (tableDefinition != null && tableDefinition.getRows() != null) {
            List<List<String>> filteredRows = new ArrayList<>();
            for (List<String> row : tableDefinition.getRowsAsList()) {
                // Vérifie si la valeur de la colonne spécifiée correspond à la valeur
                // recherchée
                int columnIndex = tableDefinition.getColumnIndex(column);
                if (columnIndex != -1 && row.size() > columnIndex && value.equals(row.get(columnIndex))) {
                    filteredRows.add(row);
                }
            }
            return ResponseEntity.ok(filteredRows);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    // select group by
    @GetMapping("/groupBy/{tableName}")
    public ResponseEntity<Map<String, Object>> groupBy(
            @PathVariable String tableName,
            @RequestParam String groupByColumn,
            @RequestParam String aggregateFunction,
            @RequestParam String aggregateColumn) {
        try {
            // Effectuer l'agrégation des données en fonction de la colonne spécifiée
            double result = tableService.aggregate(tableName, aggregateFunction, aggregateColumn);

            // Préparer la réponse
            Map<String, Object> response = new HashMap<>();
            response.put("groupedBy", groupByColumn);
            response.put("aggregateFunction", aggregateFunction);
            response.put("aggregateColumn", aggregateColumn);
            response.put("result", result);

            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            // Gérer les cas où la table spécifiée n'existe pas ou les colonnes sont
            // invalides
            return ResponseEntity.badRequest().body(Collections.singletonMap("error", e.getMessage()));
        }
    }

    private List<String> parseDataNode(JsonNode dataNode) {
        List<String> rowData = new ArrayList<>();
        dataNode.forEach(node -> rowData.add(node.asText()));
        return rowData;
    }


    @PostMapping("/executeQuery")
    public ResponseEntity<?> executeQuery(@RequestBody Select query) {
        try {
            List<List<String>> result = executeSQLQuery(query);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error processing query: " + e.getMessage());
        }
    }

    private List<List<String>> executeSQLQuery(Select query) {
        List<List<String>> result = new ArrayList<>();

        // Étape 1: SELECT * FROM tableName
        TableDefinition tableDefinition = tableService.retrieveTableData(query.getFrom());
        if (tableDefinition != null) {
            result = tableDefinition.getRowsAsList();
        } else {
            throw new IllegalArgumentException("Table " + query.getFrom() + " not found");
        }

        // Étape 2: WHERE conditions
        if (query.getWhere() != null && !query.getWhere().isEmpty()) {
            result = applyWhereConditions(result, query.getWhere(), query.getFrom());
        }

        // Étape 3: GROUP BY
        if (query.getGroupBy() != null && !query.getGroupBy().isEmpty()) {
            result = applyGroupBy(result, query.getGroupBy(), query.getColumns(), query.getFrom());
        }

        return result;
    
    }

    private List<List<String>> applyWhereConditions(List<List<String>> data, List<Where> conditions, String tableName) {
        return data.stream()
                .filter(row -> conditions.stream()
                        .allMatch(condition -> {
                            int columnIndex = getColumnIndex(tableName, condition.getColumn());
                            if (columnIndex == -1) {
                                return false;
                            }
                            String cellValue = row.get(columnIndex);
                            switch (condition.getOperand()) {
                                case "=":
                                    return cellValue.equals(condition.getValue().toString());
                                case "!=":
                                    return !cellValue.equals(condition.getValue().toString());
                                case "<":
                                    return Double.parseDouble(cellValue) < Double.parseDouble(condition.getValue().toString());
                                case "<=":
                                    return Double.parseDouble(cellValue) <= Double.parseDouble(condition.getValue().toString());
                                case ">":
                                    return Double.parseDouble(cellValue) > Double.parseDouble(condition.getValue().toString());
                                case ">=":
                                    return Double.parseDouble(cellValue) >= Double.parseDouble(condition.getValue().toString());
                                default:
                                    return false;
                            }
                        }))
                .collect(Collectors.toList());
    }

    private List<List<String>> applyGroupBy(List<List<String>> data, String groupByColumn, List<ColumnInput> columns, String tableName) {
        int groupByColumnIndex = getColumnIndex(tableName, groupByColumn);
        Map<String, List<List<String>>> groupedData = data.stream()
                .collect(Collectors.groupingBy(row -> row.get(groupByColumnIndex)));

        List<List<String>> result = new ArrayList<>();

        groupedData.forEach((key, rows) -> {
            List<String> aggregatedRow = new ArrayList<>();
            aggregatedRow.add(key);

            for (ColumnInput column : columns) {
                if (column.getAggregate() != null) {
                    int columnIndex = getColumnIndex(tableName, column.getColumn());
                    switch (column.getAggregate().toUpperCase()) {
                        case "SUM":
                            double sum = rows.stream()
                                    .mapToDouble(row -> Double.parseDouble(row.get(columnIndex)))
                                    .sum();
                            aggregatedRow.add(String.valueOf(sum));
                            break;
                        case "AVG":
                            double avg = rows.stream()
                                    .mapToDouble(row -> Double.parseDouble(row.get(columnIndex)))
                                    .average()
                                    .orElse(0);
                            aggregatedRow.add(String.valueOf(avg));
                            break;
                        case "COUNT":
                            int count = rows.size();
                            aggregatedRow.add(String.valueOf(count));
                            break;
                        // Ajoutez d'autres fonctions d'agrégation si nécessaire
                    }
                } else {
                    aggregatedRow.add(""); // Ajoutez une valeur par défaut pour les colonnes sans agrégation
                }
            }

            result.add(aggregatedRow);
        });

        return result;
    }

    private int getColumnIndex(String tableName, String columnName) {
        TableDefinition tableDefinition = tableService.retrieveTableData(tableName);
        if (tableDefinition != null) {
            List<Column> columns = tableDefinition.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                    return i;
                }
            }
        }
        throw new IllegalArgumentException("Column " + columnName + " not found in table " + tableName);
    }

    // New endpoint to retrieve data by indexed column
    @GetMapping("/retrieveByIndexedColumn/{tableName}")
    public ResponseEntity<List<Map<String, String>>> retrieveDataByIndexedColumn(
            @PathVariable String tableName,
            @RequestParam String columnName,
            @RequestParam String value) {

        try {
            List<Map<String, String>> result = tableService.retrieveDataByIndexedColumn(tableName, columnName, value);
            if (result.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Collections.singletonList(Collections.singletonMap("error", "Failed to retrieve data: " + e.getMessage())));
        }
    }
    

    

}
