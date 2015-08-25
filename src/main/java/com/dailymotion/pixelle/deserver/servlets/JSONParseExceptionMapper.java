package com.dailymotion.pixelle.deserver.servlets;

import com.fasterxml.jackson.core.JsonParseException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Created by n.dhupia on 8/25/15.
 */
@Provider
public class JSONParseExceptionMapper implements ExceptionMapper<JsonParseException> {
    @Override
    public Response toResponse(final JsonParseException jpe) {
        // Create and return an appropriate response here
        return Response.status(Response.Status.BAD_REQUEST)
                .entity("Invalid data supplied for request").build();
    }
}