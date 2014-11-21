package com.dailymotion.pixelle.deserver.servlets; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
public class DEServlet {
    private static Logger logger = LoggerFactory.getLogger(DEServlet.class);
    private DEProcessor processor;
    private HealthCheck health;


    @Inject
    public DEServlet(DEProcessor processor, HealthCheck health) {
        this.processor = processor;
        this.health = health;
    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterHealthResponse healthCheck(@QueryParam("success") String success) throws DeException {
        return health.getHealthCheck();
    }

    @POST
    @Path("/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(SearchQueryRequest sq, @QueryParam("positions") int pos, @QueryParam("allowed_types") String allowedTypes) throws DeException {
        pos = pos <= 0 ? 1 : pos;
        //default allowed type is promoted
        allowedTypes = allowedTypes == null || allowedTypes.trim() == "" ? "promoted" : allowedTypes;
        String[] at = StringUtils.split(allowedTypes);
        ItemsResponse i = processor.recommend(sq, pos, at);
        //if response is null then then return 200 with the query string in the body
        if (i == null) {
            logger.info("No ads returned =======> " + sq.toString());
            return Response.ok(sq.toString()).build();
        } else {
            logger.info("Success =======> " + i.toString());
            return Response.ok(i).build();
        }
    }

    @PUT
    @Path("/upsert")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response update(AdUnit unit) throws DeException {
        boolean isCreated = false;
        try {
            isCreated = processor.updateAdUnit(unit);
        } catch (DeException e) {
            logger.error(e.getMessage());
            throw e;
        }
        if (isCreated) {
            return Response.noContent().build();
        } else {
            logger.error("Error updating ad unit ===>" + unit.toString());
            throw new DeException(new Throwable("Error updating ad unit"), 500);
        }
    }

    @POST
    @Path("/upsert")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response insert(AdUnit unit) throws DeException {
        boolean isCreated = false;
        try {
            isCreated = processor.insertAdUnit(unit);
        } catch (DeException e) {
            logger.error(e.getMessage());
            throw e;
        }
        if (isCreated == true) {
            return Response.noContent().build();
        } else {
            logger.error("Error inserting ad unit ===>" + unit.toString());
            throw new DeException(new Throwable("Error inserting ad unit"), 500);
        }
    }
}
