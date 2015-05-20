package com.dailymotion.pixelle.deserver.processor.service;

import com.dailymotion.pixelle.deserver.model.ChannelVideos;
import com.dailymotion.pixelle.deserver.processor.DeException;
import feign.Param;
import feign.RequestLine;

/**
 * Created by n.dhupia on 3/13/15.
 */

public interface DMApiService {
    @RequestLine("GET /user/{channel}/videos?fields=id,3d,ads,allow_embed,channel,owner.screenname,created_time,updated_time,description,duration,explicit,geoblocking,language,mediablocking,mode,owner.id,owner.username,published,status,tags,thumbnail_url,title,&sort=recent&limit=25")
    ChannelVideos getVideos(@Param("channel") String channelId) throws DeException;
}