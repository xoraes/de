package com.dailymotion.pixelle.deserver.servlets; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.deserver.model.*;
import com.dailymotion.pixelle.deserver.processor.DEProcessor;
import com.dailymotion.pixelle.deserver.processor.DeException;
import com.dailymotion.pixelle.deserver.processor.DeHelper;
import com.dailymotion.pixelle.deserver.processor.hystrix.*;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.http.HttpStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.servlet.annotation.WebServlet;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/")
@WebServlet(asyncSupported = true)
public class DEServlet {
    private static Logger logger = LoggerFactory.getLogger(DEServlet.class);
    private final DEProcessor deProcessor;

    @Inject
    public DEServlet(DEProcessor deProcessor) {
        this.deProcessor = deProcessor;
    }

    /**
     * Returns the status of promoted and organic index.
     *
     * @return Object representing the health of the cluster
     * @throws DeException
     */
    @GET
    @Path("/healthcheck")
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterHealthResponse healthCheck() throws DeException {
        return deProcessor.getHealthCheck();
    }

    /**
     * Returns the status of promoted and organic index.
     *
     * @return same as healthcheck
     * @throws DeException
     */
    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    public ClusterHealthResponse status() throws DeException {
        return healthCheck();
    }

    /**
     * Returns the adunit given an adunit id.
     *
     * @param id adunit id
     * @return adunit
     * @throws DeException
     */
    @GET
    @Path("/adunit")
    @Produces(MediaType.APPLICATION_JSON)
    public AdUnit getAdUnitById(@QueryParam("id") String id) throws DeException {
        return DEProcessor.getAdUnitById(id);
    }

    /**
     * Get all ad units for a given campaign id. If campaign id is blank, return all adunits.
     *
     * @param cid - the campaign id
     * @return adunits related to a campaign. If cid is blank, then return all adunits
     * @throws DeException
     */
    @GET
    @Path("/adunits")
    @Produces(MediaType.APPLICATION_JSON)
    public List<AdUnit> getAdUnitsByCampaign(@QueryParam("cid") String cid) throws DeException {
        if (StringUtils.isBlank(cid)) {
            return DEProcessor.getAllAdUnits();
        }
        return DEProcessor.getAdUnitsByCampaign(cid);
    }

    /**
     * Delete the ad units given its id.
     *
     * @param id the adunit id
     * @return response with status code 204 if delete is success otherwise 200
     * @throws DeException
     */
    @DELETE
    @Path("/adunit")
    public Response deleteAdUnit(@QueryParam("id") String id) throws DeException {
        if (DEProcessor.deleteById(DeHelper.promotedIndex.get(), DeHelper.adunitsType.get(), id)) {
            return Response.noContent().build();
        } else {
            return Response.ok(id + " not found").build();
        }
    }

    /**
     * Given a json formatted search query, and positions - returns a list containing ad units and videos. For
     * allowedType = promoted return adunits, for allowTypes = organic return videos, for allowedTypes =
     * promoted,organic return a mix of promoted and organic videos. The order of this returned mixed list of
     * organic and promoted videos is based on a predefined pattern configured using the widget.pattern property.
     *
     * @param sq             - the search query object
     * @param pos            - the number of units to return
     * @param allowedTypes   - can be blank or "promoted" or "organic" or "promoted,organic"
     * @param isDebugEnabled - true if debug information is requested
     * @return Response object with json body containing items including adunits and/or organic videos as requested
     * @throws DeException
     */
    @POST
    @Path("/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)

    public Response query(SearchQueryRequest sq,
                          @QueryParam("positions") Integer pos,
                          @QueryParam("type") String allowedTypes,
                          @QueryParam("debug") boolean isDebugEnabled) throws DeException {
        if (isDebugEnabled) {
            sq.setDebugEnabled(true);
        }
        ItemsResponse i = null;

        try {
            i = new QueryCommand(sq, pos, allowedTypes).execute();
        } catch (HystrixBadRequestException e) {
            throw new DeException(HttpStatus.BAD_REQUEST_400, "Bad Request");
        }
        return Response.ok(i).build();
    }

    /**
     * Asynchronously inserts a list of videos to the promoted index.
     *
     * @param adUnits
     * @param ar
     * @throws DeException
     */
    @POST
    @Path("/adunits")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void insertAdUnitsBulk(List<AdUnit> adUnits, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new AdUnitBulkInsertCommand(adUnits).execute());
    }

    /**
     * Asynchronously updates an ad unit to the promoted index.
     *
     * @param adunit
     * @param ar
     * @throws DeException
     */
    @PUT
    @Path("/adunit")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void update(AdUnit adunit, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new AdUpdateCommand(adunit).execute());
    }

    /**
     * Asynchronously inserts a ad unit to the promoted index.
     *
     * @param adunit
     * @param ar
     * @throws DeException
     */
    @POST
    @Path("/adunit")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void insert(AdUnit adunit, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new AdInsertCommand(adunit).execute());
    }

    /**
     * Asynchronously updates a list of videos to the organic index.
     *
     * @param videos
     * @param ar
     * @throws DeException
     */
    @PUT
    @Path("/videos")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void updateBulk(List<Video> videos, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new VideoBulkInsertCommand(videos).execute());
    }

    /**
     * Asynchronously inserts a list of videos to the organic index.
     *
     * @param videos
     * @param ar
     * @throws DeException
     */
    @POST
    @Path("/videos")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public void insertVideosBulk(List<Video> videos, @Suspended final AsyncResponse ar) throws DeException {
        ar.resume(new VideoBulkInsertCommand(videos).execute());
    }

    /**
     * Returns info for a given video id.
     *
     * @param id - video id
     * @return Video
     * @throws DeException
     */
    @GET
    @Path("/video")
    @Produces(MediaType.APPLICATION_JSON)
    public Video getVideoById(@QueryParam("id") String id) throws DeException {
        return DEProcessor.getVideoById(id);
    }

    /**
     * Deletes video from the index.
     *
     * @param id - video id
     * @return response with status code 204 if delete is success otherwise 200
     * @throws DeException
     */
    @DELETE
    @Path("/video")
    public Response deleteVideoById(@QueryParam("id") String id) throws DeException {
        if (DEProcessor.deleteById(DeHelper.organicIndex.get(), DeHelper.videosType.get(), id)) {
            return Response.noContent().build();
        } else {
            return Response.ok(id + " not found").build();
        }
    }

    /**
     * Forecast daily and total views based on given data.
     *
     * @param forecastRequest
     * @throws DeException
     */
    @POST
    @Path("/forecast")
    @ManagedAsync
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public ForecastResponse forecast(ForecastRequest forecastRequest) throws DeException {
        return DEProcessor.forecast(forecastRequest);
    }
}
