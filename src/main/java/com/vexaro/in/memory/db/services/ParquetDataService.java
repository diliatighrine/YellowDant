package com.vexaro.in.memory.db.services;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Service
public class ParquetDataService {
    private static final int batchSize = 100000;
    private boolean compressData; // Indicateur de compression des données



    public void setCompressData(boolean compressData) {
        this.compressData = compressData;
    }


    public void parseParquetFile(String filePath, Consumer<Map<Integer, Map<String, String>>> dataConsumer) {
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(filePath)).build()) {
            Group row;
            Map<Integer, Map<String, String>> rows = new HashMap<>();
            int cpt = 0;
            while (((row = reader.read()) != null) && (cpt != 1000000)) {
                Map<String, String> rowData = parseRow(row);
                if (compressData) {
                    rowData = compressData(rowData);
                }
                rows.put(cpt, rowData);
                cpt++;
                //afficher la ligne lue
                //System.out.println(rowData);

                if (rows.size() >= batchSize) {
                    dataConsumer.accept(rows);
                    rows = new HashMap<>();
                }
            }
            if (!rows.isEmpty()) {
                dataConsumer.accept(rows);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    } 

    private Map<String, String> parseRow(Group group) {
        Map<String, String> rowData = new HashMap<>();
        MessageType schema = (MessageType) group.getType();
        for (Type field : schema.getFields()) {
            int fieldIndex = group.getType().getFieldIndex(field.getName());
            if (group.getFieldRepetitionCount(fieldIndex) == 0) {
                rowData.put(field.getName(), null);
                continue;
            }
            if (field.isPrimitive()) {
                PrimitiveType.PrimitiveTypeName typeName = field.asPrimitiveType().getPrimitiveTypeName();
                String value;
                switch (typeName) {
                    case BINARY:
                        value = group.getBinary(field.getName(), 0).toStringUsingUTF8();
                        break;
                    case INT32:
                        value = String.valueOf(group.getInteger(field.getName(), 0));
                        break;
                    case INT64:
                        value = String.valueOf(group.getLong(field.getName(), 0));
                        break;
                    case DOUBLE:
                        value = String.valueOf(group.getDouble(field.getName(), 0));
                        break;
                    case FLOAT:
                        value = String.valueOf(group.getFloat(field.getName(), 0));
                        break;
                    case BOOLEAN:
                        value = String.valueOf(group.getBoolean(field.getName(), 0));
                        break;
                    default:
                        throw new IllegalStateException("Unsupported type: " + typeName);
                }
                rowData.put(field.getName(), value);
            }
        }
        return rowData;
    }

    private Map<String, String> compressData(Map<String, String> rowData) {
        
        // use GZIP compression
        try {
            StringBuilder compressedData = new StringBuilder();
            for (Map.Entry<String, String> entry : rowData.entrySet()) {
                compressedData.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            String compressedString = compressedData.toString();
            byte[] compressedBytes = compressedString.getBytes();
            return Map.of("compressedData", new String(compressedBytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowData;
    }
}

