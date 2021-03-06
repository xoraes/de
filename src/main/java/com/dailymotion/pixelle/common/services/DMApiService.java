package com.dailymotion.pixelle.common.services;

import com.dailymotion.pixelle.de.model.DMApiResponse;
import com.dailymotion.pixelle.de.processor.DeException;
import feign.Param;
import feign.RequestLine;

/**
 * Created by n.dhupia on 3/13/15.
 */

public interface DMApiService {
    @RequestLine("GET /videos?owners={channels}&fields=id,3d,ads,allow_embed,channel,owner.screenname,created_time," +
            "updated_time,description,duration,explicit,geoblocking,language,mediablocking,mode,owner.id,owner" +
            ".username,published,status,tags,thumbnail_url,title,&sort={order}&limit=50")
    DMApiResponse getChannelVideos(@Param("channels") String channelIds, @Param("order") String sortOrder) throws
            DeException;

    @RequestLine("GET /playlist/{playlist}/videos?fields=id,3d,ads,allow_embed,channel,owner" +
            ".screenname,created_time," +
            "updated_time,description,duration,explicit,geoblocking,language,mediablocking,mode,owner.id,owner" +
            ".username,published,status,tags,thumbnail_url,title,&sort={order}&limit=50")
    DMApiResponse getPlaylistVideos(@Param("playlist") String playlist, @Param("order") String sortOrder) throws
            DeException;
}