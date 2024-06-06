package com.vexaro.in.memory.db.services;
import com.vexaro.in.memory.db.dto.PaginatedTableData;
import com.vexaro.in.memory.db.dto.TableRowsRequest;
import com.vexaro.in.memory.db.models.Column;
import com.vexaro.in.memory.db.models.TableDefinition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Slf4j
@Service
@RequiredArgsConstructor
public class TableService {
    private final ParquetDataService parquetDataService;
    private Map<String, TableDefinition> inMemoryDatabase = new HashMap<>();

    public void createSchema(TableDefinition tableDefinition) {
        inMemoryDatabase.put(tableDefinition.getName(), tableDefinition);
        log.info("'{}' schema created successfully", tableDefinition.getName());
    }

    // Updated loadTableData method
    public boolean loadTableData(String tableName, Map<Integer, Map<String, String>> data) {
        TableDefinition tableDefinition = inMemoryDatabase.get(tableName);
        if (tableDefinition != null) {
            for (Map.Entry<Integer, Map<String, String>> entry : data.entrySet()) {
                Map<String, String> rowData = entry.getValue();
                if (rowData.containsKey("compressedData")) {
                    rowData = decompressData(rowData);
                }
                tableDefinition.addRow(rowData);
            }
            return true;
        } else {
            return false;
        }
    }

    public PaginatedTableData retrieveTableDataWithPagination(String tableName, int offset, int limit) {
        TableDefinition tableDefinition = inMemoryDatabase.get(tableName);
        if (tableDefinition == null) {
            throw new IllegalArgumentException("The table with the given name does not exist");
        }
        Map<Integer, Map<String, String>> fullTableData = tableDefinition.getRows();
        if (fullTableData == null || fullTableData.isEmpty()) {
            throw new IllegalArgumentException("The table with the given name does not have any data");
        }

        int totalEntries = fullTableData.size();
        int fromIndex = Math.min(offset, totalEntries);
        int toIndex = Math.min(offset + limit, totalEntries);

        List<Map<String, String>> paginatedRows = new ArrayList<>();

        for (int i = fromIndex; i < toIndex; i++) {
            Map<String, String> row = fullTableData.get(i);
            if (row != null ) {
                if (row.containsKey("compressedData")) {
                    row = decompressData(row);
                }

            
                paginatedRows.add(row);
            }
        }

        PaginatedTableData paginatedTableData = new PaginatedTableData();
        paginatedTableData.setRows(paginatedRows);
        paginatedTableData.setTotalEntries(totalEntries);
        paginatedTableData.setTotalPages((totalEntries + limit - 1) / limit);

        return paginatedTableData;
    }

    private Map<String, String> decompressData(Map<String, String> rowData) {
        try {
            String compressedString = rowData.get("compressedData");
            byte[] compressedBytes = compressedString.getBytes();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedBytes);
            GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
            Scanner scanner = new Scanner(gzipInputStream, "UTF-8");
            scanner.useDelimiter("\\A");
            String decompressedString = scanner.hasNext() ? scanner.next() : "";
            scanner.close();

            Map<String, String> decompressedData = new HashMap<>();
            String[] pairs = decompressedString.split(",");
            for (String pair : pairs) {
                String[] keyValue = pair.split(":");
                if (keyValue.length == 2) {
                    decompressedData.put(keyValue[0], keyValue[1]);
                }
            }
            return decompressedData;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowData;
    }

    private Map<String, String> compressData(Map<String, String> rowData) {
        try {
            StringBuilder dataBuilder = new StringBuilder();
            for (Map.Entry<String, String> entry : rowData.entrySet()) {
                dataBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            String dataString = dataBuilder.toString();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
            gzipOutputStream.write(dataString.getBytes());
            gzipOutputStream.close();
            byte[] compressedBytes = byteArrayOutputStream.toByteArray();
            String compressedString = Base64.getEncoder().encodeToString(compressedBytes);
            return Map.of("compressedData", compressedString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowData;
    }


    private Map<String, String> mapRowToColumnName(List<Column> columns, List<String> rowData) {
        Map<String, String> rowMap = new LinkedHashMap<>();

        if (columns == null || columns.isEmpty()) {
            for (int i = 0; i < rowData.size(); i++) {
                String columnName = "column" + (i + 1); // Start naming columns with 1
                rowMap.put(columnName, rowData.get(i));
            }
        } else {
            for (int i = 0; i < columns.size(); i++) {
                log.info("Column: {}, row: {}", columns.get(i).getName(), rowData.get(i));
                rowMap.put(columns.get(i).getName(), rowData.get(i));
            }
        }
        return rowMap;
    }

    public TableDefinition retrieveTableData(String tableName) {
        return inMemoryDatabase.get(tableName);
    }

    

    public double aggregate(String tableName, String function, String columnName) {
        TableDefinition table = inMemoryDatabase.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found");
        }

        Map<Integer, Map<String, String>> rows = table.getRows();
        double result = 0;
        long count = 0;

        switch (function.toUpperCase()) {
            case "SUM":
                for (Map<String, String> row : rows.values()) {
                    if (row.containsKey("compressedData")) {
                        row = decompressData(row);
                    }
                    try {
                        double value = Double.parseDouble(row.get(columnName));
                        result += value;
                    } catch (NumberFormatException ignored) {
                    }
                }
                break;
            case "COUNT":
                for (Map<String, String> row : rows.values()) {
                    if (row.containsKey("compressedData")) {
                        row = decompressData(row);
                    }
                    String value = row.get(columnName);
                    if (value != null && !value.isEmpty()) {
                        count++;
                    }
                }
                result = count;
                break;
            
            case "AVG":
                for (Map<String, String> row : rows.values()) {
                    if (row.containsKey("compressedData")) {
                        row = decompressData(row);
                    }
                    try {
                        double value = Double.parseDouble(row.get(columnName));
                        result += value;
                        count++;
                    } catch (NumberFormatException ignored) {
                    }
                }
                if (count != 0) {
                    result /= count;
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported function: " + function);
        }
        return result;
    }



    public boolean insertData(String tableName, List<String> rowData) {
        TableDefinition tableDefinition = inMemoryDatabase.get(tableName);
        if (tableDefinition != null) {
            Map<String, String> rowMap = mapRowToColumnName(tableDefinition.getColumns(), rowData);
            if (tableDefinition.isCompressData()) {
                rowMap = compressData(rowMap);
            }
            tableDefinition.addRow(rowMap);
            return true;
        } else {
            return false;
        }
    }

    public boolean updateData(String tableName, int rowIndex, List<String> newData) {
        TableDefinition tableDefinition = inMemoryDatabase.get(tableName);
        if (tableDefinition != null) {
            Map<String, String> newRow = mapRowToColumnName(tableDefinition.getColumns(), newData);
            if (tableDefinition.isCompressData()) {
                newRow = compressData(newRow);
            }
            tableDefinition.getRows().put(rowIndex, newRow);
            return true;
        }
        return false;
    }

    public boolean deleteData(String tableName, int rowIndex) {
        TableDefinition tableDefinition = inMemoryDatabase.get(tableName);
        if (tableDefinition != null) {
            tableDefinition.getRows().remove(rowIndex);
            return true;
        }
        return false;
    }

    public void createIndex(String tableName, String columnName) {
        TableDefinition tableDefinition = inMemoryDatabase.get(tableName);
        if (tableDefinition != null) {
            tableDefinition.createIndexes(columnName);
        }
    }

    public List<Map<String, String>> retrieveDataByIndexedColumn(String tableName, String columnName, String value) {
        TableDefinition tableDefinition = inMemoryDatabase.get(tableName);
        if (tableDefinition == null) {
            throw new IllegalArgumentException("Table not found");
        }
        Set<Integer> rowIndices = tableDefinition.getRowsByIndexedColumn(columnName, value);
        List<Map<String, String>> result = new ArrayList<>();
        for (Integer rowIndex : rowIndices) {
            result.add(tableDefinition.getRows().get(rowIndex));
        }
        return result;
    }

    




}
