package com.demo.dataeng;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.Map;

@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BigTableSinkMessage {
    @JsonProperty("row_key")
    private String rowKey;
    private Map<String, Map<String, String>> row;
}