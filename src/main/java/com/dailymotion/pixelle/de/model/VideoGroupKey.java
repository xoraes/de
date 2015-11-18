package com.dailymotion.pixelle.de.model;

import lombok.Data;

/**
 * Created by n.dhupia on 11/17/15.
 */
@Data
public class VideoGroupKey {
    private String channels;
    private String sortOrder = "recent";
    private String playlist;

    public VideoGroupKey(String channels, String playlist, String sortOrder) {
        this.channels = channels;
        this.playlist = playlist;
        this.sortOrder = sortOrder;
    }
}
