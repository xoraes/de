package com.dailymotion.pixelle.common.server; /**
 * Created by n.dhupia on 10/29/14.
 */

import com.dailymotion.pixelle.de.model.AdUnit;
import com.dailymotion.pixelle.de.model.ItemsResponse;
import com.dailymotion.pixelle.de.model.SearchQueryRequest;
import com.dailymotion.pixelle.de.model.Video;
import com.dailymotion.pixelle.de.processor.DEProcessor;
import com.dailymotion.pixelle.de.processor.DeException;
import com.dailymotion.pixelle.de.processor.hystrix.AdInsertCommand;
import com.dailymotion.pixelle.de.processor.hystrix.AdUnitBulkInsertCommand;
import com.dailymotion.pixelle.de.processor.hystrix.AdUpdateCommand;
import com.dailymotion.pixelle.de.processor.hystrix.QueryCommand;
import com.dailymotion.pixelle.de.processor.hystrix.VideoBulkInsertCommand;
import com.dailymotion.pixelle.forecast.model.ForecastRequest;
import com.dailymotion.pixelle.forecast.model.ForecastResponse;
import com.dailymotion.pixelle.forecast.processor.ForecastException;
import com.dailymotion.pixelle.forecast.processor.Forecaster;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.servlet.annotation.WebServlet;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import java.util.List;

import static com.dailymotion.pixelle.de.processor.DEProcessor.deleteById;
import static com.dailymotion.pixelle.de.processor.DEProcessor.getAllAdUnits;
import static com.dailymotion.pixelle.de.processor.DeHelper.adunitsType;
import static com.dailymotion.pixelle.de.processor.DeHelper.organicIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.promotedIndex;
import static com.dailymotion.pixelle.de.processor.DeHelper.videosType;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.noContent;
import static javax.ws.rs.core.Response.ok;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.eclipse.jetty.http.HttpStatus.BAD_REQUEST_400;
import static org.slf4j.LoggerFactory.getLogger;

@Path("/")
@WebServlet(asyncSupported = true)
public class AppServlet {
    private static Logger logger = getLogger(AppServlet.class);
    private final DEProcessor deProcessor;

    @Inject
    public AppServlet(DEProcessor deProcessor) {
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
    @Produces(TEXT_PLAIN)
    public String healthCheck() throws DeException {
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
    @Produces(TEXT_PLAIN)
    public String status() throws DeException {
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
    @Produces(APPLICATION_JSON)
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
    @Produces(APPLICATION_JSON)
    public List<AdUnit> getAdUnitsByCampaign(@QueryParam("cid") String cid) throws DeException {
        if (isBlank(cid)) {
            return getAllAdUnits();
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
        if (deleteById(promotedIndex.get(), adunitsType.get(), id)) {
            return noContent().build();
        } else {
            return ok(id + " not found").build();
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
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)

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
            throw new DeException(BAD_REQUEST_400, "Bad Request");
        }
        return ok(i).build();
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
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
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
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
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
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
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
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
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
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
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
    @Produces(APPLICATION_JSON)
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
        if (deleteById(organicIndex.get(), videosType.get(), id)) {
            return noContent().build();
        } else {
            return ok(id + " not found").build();
        }
    }

    /**
     * Forecast daily and total views based on given data.
     *
     * @param forecastRequest
     * @throws ForecastException
     */
    @POST
    @Path("/forecast")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public ForecastResponse forecast(ForecastRequest forecastRequest) throws ForecastException {
        return Forecaster.forecast(forecastRequest);
    }
}
