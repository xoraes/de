package com.dailymotion.pixelle.deserver.servlets; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.processor.*;
import com.google.inject.Inject;
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
        ItemsResponse i = new AdQueryCommand(processor, sq, pos, allowedTypes).execute();
        return Response.ok(i).build();
    }

    @PUT
    @Path("/upsert")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response update(AdUnit unit) throws DeException {
        Boolean isUpdated = new AdUpdateCommand(processor, unit).execute();
        if (!isUpdated) { // this is going to be very rare unless es is buggy
            throw new DeException(new Throwable("Error updating ad unit"), 500);
        }
        return Response.noContent().build();
    }

    @POST
    @Path("/upsert")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response insert(AdUnit unit) throws DeException {
        Boolean isCreated = new AdInsertCommand(processor, unit).execute();
        if (!isCreated) { // this is going to be very rare unless es is buggy
            throw new DeException(new Throwable("Error creating ad unit"), 500);
        }
        return Response.noContent().build();
    }
}
