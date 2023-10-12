package com.an.job.liveStreaming.pojo;

import com.alibaba.fastjson.annotation.JSONField;

public class test {

    @JSONField(name = "carrier")
    private String carrier;
    @JSONField(name = "deviceId")
    private String deviceId;
    @JSONField(name = "deviceType")
    private String deviceType;
    @JSONField(name = "eventId")
    private String eventId;
    @JSONField(name = "id")
    private Integer id;
    @JSONField(name = "isNew")
    private Integer isNew;
    @JSONField(name = "lastUpdate")
    private Integer lastUpdate;
    @JSONField(name = "latitude")
    private Double latitude;
    @JSONField(name = "longitude")
    private Double longitude;
    @JSONField(name = "netType")
    private String netType;
    @JSONField(name = "osName")
    private String osName;
    @JSONField(name = "osVersion")
    private String osVersion;
    @JSONField(name = "releaseChannel")
    private String releaseChannel;
    @JSONField(name = "resolution")
    private String resolution;
    @JSONField(name = "sessionId")
    private String sessionId;
    @JSONField(name = "timestamp")
    private Long timestamp;

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getIsNew() {
        return isNew;
    }

    public void setIsNew(Integer isNew) {
        this.isNew = isNew;
    }

    public Integer getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Integer lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public String getNetType() {
        return netType;
    }

    public void setNetType(String netType) {
        this.netType = netType;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getReleaseChannel() {
        return releaseChannel;
    }

    public void setReleaseChannel(String releaseChannel) {
        this.releaseChannel = releaseChannel;
    }

    public String getResolution() {
        return resolution;
    }

    public void setResolution(String resolution) {
        this.resolution = resolution;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
