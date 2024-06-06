package com.vexaro.in.memory.db.dto;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class PaginatedTableData {
    private List<Map<String, String>> rows;
    private int totalPages;
    private long totalEntries;
}
