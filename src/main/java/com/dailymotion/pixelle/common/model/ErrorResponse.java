package com.dailymotion.pixelle.common.model;

import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.forecast.processor.ForecastException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Created by n.dhupia on 5/19/15.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ErrorResponse {
    @JsonProperty("status")
    private int status;
    @JsonProperty("message")
    private String msg;

    public ErrorResponse(DeException ex) {
        this.status = ex.getStatus();
        this.msg = ex.getMsg();
    }

    public ErrorResponse(ForecastException ex) {
        this.status = ex.getStatus();
        this.msg = ex.getMsg();
    }
}
