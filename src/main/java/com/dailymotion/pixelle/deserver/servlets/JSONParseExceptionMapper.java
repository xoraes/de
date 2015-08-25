package com.dailymotion.pixelle.deserver.servlets;

import com.dailymotion.pixelle.deserver.model.ErrorResponse;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.fasterxml.jackson.core.JsonParseException;
import org.eclipse.jetty.http.HttpStatus;

import javax.ws.rs.core.MediaType;
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
        DeException dex = new DeException(HttpStatus.BAD_REQUEST_400, "Invalid request");
        ErrorResponse ex = new ErrorResponse(dex);
        return Response.status(HttpStatus.BAD_REQUEST_400)
                .entity(ex).type(MediaType.APPLICATION_JSON).build();
    }
}