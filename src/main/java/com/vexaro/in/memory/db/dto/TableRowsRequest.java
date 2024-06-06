package com.vexaro.in.memory.db.dto;

import lombok.Data;

import java.util.List;

@Data
public class TableRowsRequest {
    private List<List<String>> rows;
}
