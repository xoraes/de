package com.dailymotion.pixelle.deserver.model;

import com.dailymotion.pixelle.deserver.processor.DeException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by n.dhupia on 10/30/14.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY) //this will ensure only non-null values are included in the serialized json
public class ItemsResponse {
    private static Logger logger = LoggerFactory.getLogger(ItemsResponse.class);
    @JsonProperty("_items")
    private List<AdUnitResponse> adUnitResponse;

    public List<AdUnitResponse> getAdUnitResponse() {
        return adUnitResponse;
    }

    public void setAdUnitResponse(List<AdUnitResponse> adUnitResponse) {
        this.adUnitResponse = adUnitResponse;
    }

    public String toString() {
        ObjectMapper mapper = new ObjectMapper();

        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.error("error parsing json", e);
            throw new DeException(e, 500);
        }
    }
}
