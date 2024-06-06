package com.vexaro.in.memory.db.models;

import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

@Data
public class TableDefinition {
    private String name;
    private List<Column> columns;
    private Map<Integer, Map<String, String>> rows = new HashMap<>();
    private int rowCounter = 0; // To keep track of row indices
    // Index structures
    private Map<String, Map<String, Set<Integer>>> indexes = new HashMap<>();
    private boolean compressData;
    
    public int getColumnIndex(String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1; // Column not found
    }

    public void addRow(Map<String, String> rowData) {
        rows.put(rowCounter++, rowData);

        for (Map.Entry<String, String> entry : rowData.entrySet()) {
            String column = entry.getKey();
            String value = entry.getValue();
            indexes.computeIfAbsent(column, k -> new HashMap<>())
                   .computeIfAbsent(value, k -> new HashSet<>())
                   .add(rowCounter);
        }
        rowCounter++;
    }

    public Map<Integer, Map<String, String>> getRows() {
        return rows;
    }

    public void createIndexes(String columnName) {
        // Créer les index sur les colonnes spécifiées
        if (!indexes.containsKey(columnName)) {
            indexes.put(columnName, new HashMap<>());
            for (Map.Entry<Integer, Map<String, String>> entry : rows.entrySet()) {
                Integer rowIndex = entry.getKey();
                String value = entry.getValue().get(columnName);
                indexes.get(columnName).computeIfAbsent(value, k -> new HashSet<>()).add(rowIndex);
            }
        }
    }

    public Set<Integer> getRowsByIndexedColumn(String columnName, String value) {
        return indexes.getOrDefault(columnName, Collections.emptyMap()).getOrDefault(value, Collections.emptySet());
    }

    public List<List<String>> getRowsAsList() {
        List<List<String>> listOfRows = new ArrayList<>();
        for (Map<String, String> rowMap : rows.values()) {
            List<String> rowList = new ArrayList<>(rowMap.values());
            listOfRows.add(rowList);
        }
        return listOfRows;
    }

    public void compressData() throws IOException {
        for (Map.Entry<Integer, Map<String, String>> entry : rows.entrySet()) {
            Integer rowIndex = entry.getKey();
            Map<String, String> row = entry.getValue();
            byte[] compressedRow = compressRow(row);
            rows.put(rowIndex, decompressRow(compressedRow));
        }
    }

    private byte[] compressRow(Map<String, String> row) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(gzipOutputStream)) {
            objectOutputStream.writeObject(row);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private Map<String, String> decompressRow(byte[] compressedRow) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedRow);
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
             ObjectInputStream objectInputStream = new ObjectInputStream(gzipInputStream)) {
            return (Map<String, String>) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to decompress row", e);
        }
    }


}
