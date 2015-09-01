package com.dailymotion.pixelle.de.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.slf4j.Logger;

import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by n.dhupia on 10/30/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
@Data
public class ItemsResponse {
    private static Logger LOGGER = getLogger(ItemsResponse.class);

    @JsonProperty("_items")
    private List<? extends ItemsResponse> response;
}
