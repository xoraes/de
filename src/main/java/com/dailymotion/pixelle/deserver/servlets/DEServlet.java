package com.dailymotion.pixelle.deserver.servlets; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.model.AdUnit;
import com.dailymotion.pixelle.deserver.model.ItemsResponse;
import com.dailymotion.pixelle.deserver.model.SearchQueryRequest;
import com.dailymotion.pixelle.deserver.model.Video;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.dailymotion.pixelle.deserver.processor.hystrix.*;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/")
public class DEServlet {
    private static Logger logger = LoggerFactory.getLogger(DEServlet.class);
    private DEProcessor deProcessor;

    @Inject
    public DEServlet(DEProcessor deProcessor) {
        this.deProcessor = deProcessor;
    }

    @GET
    @Path("/healthcheck")
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterHealthResponse healthCheck() throws DeException {
        return deProcessor.getHealthCheck();
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
        return deProcessor.getAdUnitById(id);
    }

    @GET
    @Path("/adunits")
    @Produces(MediaType.APPLICATION_JSON)
    public List<AdUnit> getAdUnitsByCampaign(@QueryParam("cid") String cid) throws DeException {
        if (StringUtils.isBlank(cid)) {
            return deProcessor.getAllAdUnits();
        }
        return deProcessor.getAdUnitsByCampaign(cid);
    }

    @POST
    @Path("/index")
    public Response createIndexWithTypes() throws DeException {
        deProcessor.createIndexWithTypes();
        return Response.noContent().build();
    }

    @DELETE
    @Path("/index")
    public Response deleteIndex() throws DeException {
        deProcessor.deleteIndex();
        return Response.noContent().build();
    }

    @DELETE
    @Path("/adunit")
    public Response deleteAdUnit(@QueryParam("id") String id) throws DeException {
        if (deProcessor.deleteById(DeHelper.getIndex(), DeHelper.getAdUnitsType(), id)) {
            return Response.noContent().build();
        } else {
            return Response.ok(id + " not found").build();
        }
    }

    @GET
    @Path("/video/lastdatetime")
    @Produces(MediaType.TEXT_PLAIN)
    public String getVideoLastTimeStamp(@QueryParam("type") String type) throws DeException {
        return deProcessor.getLastUpdatedTimeStamp(DeHelper.getOrganicVideoType());
    }

    @GET
    @Path("/adunits/lastdatetime")
    @Produces(MediaType.TEXT_PLAIN)
    public String getAdLastTimeStamp(@QueryParam("type") String type) throws DeException {
        return deProcessor.getLastUpdatedTimeStamp(DeHelper.getAdUnitsType());
    }

    @POST
    @Path("/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(SearchQueryRequest sq, @QueryParam("positions") Integer pos, @QueryParam("type") String allowedTypes, @QueryParam("debug") Boolean isDebugEnabled) throws DeException {
        if (isDebugEnabled == null) {
            sq.setDebugEnabled(false);
        } else {
            sq.setDebugEnabled(isDebugEnabled);
        }
        ItemsResponse i = new QueryCommand(deProcessor, sq, pos, allowedTypes).execute();
        return Response.ok(i).build();
    }

    @POST
    @Path("/adunits")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void insertAdUnitsBulk(List<AdUnit> adUnits, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new AdUnitBulkInsertCommand(deProcessor, adUnits).execute());
    }

    @PUT
    @Path("/adunit")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void update(AdUnit unit, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new AdUpdateCommand(deProcessor, unit).execute());
    }

    @POST
    @Path("/adunit")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void insert(AdUnit unit, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new AdInsertCommand(deProcessor, unit).execute());
    }

    @PUT
    @Path("/video")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void update(Video video, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new VideoUpdateCommand(deProcessor, video).execute());
    }

    @PUT
    @Path("/videos")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void updateBulk(List<Video> videos, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new VideoBulkInsertCommand(deProcessor, videos).execute());
    }

    @POST
    @Path("/videos")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void insertVideosBulk(List<Video> videos, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new VideoBulkInsertCommand(deProcessor, videos).execute());
    }


    @POST
    @ManagedAsync
    @Path("/video")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void insert(Video video, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new VideoInsertCommand(deProcessor, video).execute());
    }

    @GET
    @Path("/video")
    @Produces(MediaType.APPLICATION_JSON)
    public Video getVideoById(@QueryParam("id") String id) throws DeException {
        return deProcessor.getVideoById(id);
    }

    @DELETE
    @Path("/video")
    public Response deleteVideoById(@QueryParam("id") String id) throws DeException {
        if (deProcessor.deleteById(DeHelper.getIndex(), DeHelper.getOrganicVideoType(), id)) {
            return Response.noContent().build();
        } else {
            return Response.ok(id + " not found").build();
        }
    }
}
