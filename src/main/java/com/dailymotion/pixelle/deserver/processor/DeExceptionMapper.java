package com.dailymotion.pixelle.deserver.processor;

import com.dailymotion.pixelle.deserver.model.ErrorResponse;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;


/**
 * Created by n.dhupia on 5/19/15.
 */
@Provider
public class DeExceptionMapper implements ExceptionMapper<DeException> {

    public Response toResponse(DeException ex) {
        return Response.status(ex.getStatus())
                .entity(new ErrorResponse(ex))
                .type(MediaType.APPLICATION_JSON)
                .build();
    }
}