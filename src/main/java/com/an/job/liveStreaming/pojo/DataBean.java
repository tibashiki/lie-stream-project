package com.an.job.liveStreaming.pojo;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.HashMap;

public class DataBean {
    @JSONField(name = "carrier")
    private String carrier;
    @JSONField(name = "deviceId")
    private String deviceId;
    @JSONField(name = "deviceType")
    private String deviceType;
    @JSONField(name = "eventId")
    private String eventId;
    private String id;
    @JSONField(name = "isNew")
    private Integer isNew;
    @JSONField(name = "lastUpdate")
    private long lastUpdate;
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

    private String guid;

    private String account;

    private Integer isN;

    private String appId;

    private String appVersion;

    private String ip;

    private String newSessionId;

    private String country;

    private String province;

    private String city;

    private String region;

    private String date;

    private String hour;

    private HashMap<String, Object> properties;

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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getIsNew() {
        return isNew;
    }

    public void setIsNew(Integer isNew) {
        this.isNew = isNew;
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

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getNewSessionId() {
        return newSessionId;
    }

    public void setNewSessionId(String newSessionId) {
        this.newSessionId = newSessionId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public HashMap<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, Object> properties) {
        this.properties = properties;
    }

    public Integer getIsN() {
        return isN;
    }

    public void setIsN(Integer isN) {
        this.isN = isN;
    }

    @Override
    public String toString() {
        return "DataBean{" +
                "carrier='" + carrier + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", eventId='" + eventId + '\'' +
                ", id=" + id +
                ", isNew=" + isNew +
                ", lastUpdate=" + lastUpdate +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", netType='" + netType + '\'' +
                ", osName='" + osName + '\'' +
                ", osVersion='" + osVersion + '\'' +
                ", releaseChannel='" + releaseChannel + '\'' +
                ", resolution='" + resolution + '\'' +
                ", sessionId='" + sessionId + '\'' +
                ", timestamp=" + timestamp +
                ", guid='" + guid + '\'' +
                ", account='" + account + '\'' +
                ", appId='" + appId + '\'' +
                ", appVersion='" + appVersion + '\'' +
                ", ip='" + ip + '\'' +
                ", newSessionId='" + newSessionId + '\'' +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", region='" + region + '\'' +
                ", date='" + date + '\'' +
                ", hour='" + hour + '\'' +
                ", properties=" + properties +
                '}';
    }
}
