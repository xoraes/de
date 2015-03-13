package com.dailymotion.pixelle.deserver.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 10/30/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
@Data
public class ItemsResponse {
    private static Logger logger = LoggerFactory.getLogger(ItemsResponse.class);

    @JsonProperty("_items")
    private List<? extends ItemsResponse> response;
}
