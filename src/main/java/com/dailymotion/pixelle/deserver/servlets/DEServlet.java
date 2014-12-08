package com.dailymotion.pixelle.deserver.servlets; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.processor.*;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/")
public class DEServlet {
    private static Logger logger = LoggerFactory.getLogger(DEServlet.class);
    private DEProcessor processor;


    @Inject
    public DEServlet(DEProcessor processor) {
        this.processor = processor;
    }

    @GET
    @Path("/healthcheck")
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterHealthResponse healthCheck() throws DeException {
        return processor.getHealthCheck();
    }

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterHealthResponse status() throws DeException {
        return healthCheck();
    }

    @GET
    @Path("/adunit")
    @Produces(MediaType.APPLICATION_JSON)
    public AdUnit getAdUnitById(@QueryParam("id") String id) throws DeException {
        return processor.getAdUnitById(id);
    }

    @GET
    @Path("/adunits")
    @Produces(MediaType.APPLICATION_JSON)
    public List<AdUnit> getAdUnitsByCampaign(@QueryParam("cid") String cid) throws DeException {
        if (StringUtils.isBlank(cid)) {
            return processor.getAllAdUnits();
        }
        return processor.getAdUnitsByCampaign(cid);
    }

    @POST
    @Path("/index")
    public Response createAdIndexWithType() throws DeException {
        processor.createAdIndexWithType();
        return Response.noContent().build();
    }

    @DELETE
    @Path("/index")
    public Response deleteIndex() throws DeException {
        processor.deleteIndex();
        return Response.noContent().build();
    }

    @DELETE
    @Path("/adunit")
    public Response deleteAdUnit(@QueryParam("id") String id) throws DeException {
        if (processor.deleteById(DeHelper.getIndex(), DeHelper.getAdUnitsType(), id)) {
            return Response.noContent().build();
        } else {
            return Response.ok(id + " not found").build();
        }
    }

    @GET
    @Path("/lastdatetime")
    @Produces(MediaType.TEXT_PLAIN)
    public String getLastTimeStamp(@QueryParam("type") String type) throws DeException {
        return processor.getLastUpdatedTimeStamp(type);
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
        new AdUpdateCommand(processor, unit).execute();
        return Response.noContent().build();
    }

    @POST
    @Path("/upsert")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response insert(AdUnit unit) throws DeException {
        new AdInsertCommand(processor, unit).execute();
        return Response.noContent().build();
    }
}
