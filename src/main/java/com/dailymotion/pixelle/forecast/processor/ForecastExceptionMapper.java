package com.dailymotion.pixelle.forecast.processor;

import com.dailymotion.pixelle.common.model.ErrorResponse;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;


/**
 * Created by n.dhupia on 5/19/15.
 */
@Provider
public class ForecastExceptionMapper implements ExceptionMapper<ForecastException> {

    public Response toResponse(ForecastException ex) {
        return Response.status(ex.getStatus())
                .entity(new ErrorResponse(ex))
                .type(MediaType.APPLICATION_JSON)
                .build();
    }
}