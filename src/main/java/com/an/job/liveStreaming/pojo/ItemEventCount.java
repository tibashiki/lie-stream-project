package com.an.job.liveStreaming.pojo;

public class ItemEventCount {
    public String productId;  //商品ID
    public String eventId;  //事件ID
    public String categoryId;  //分类ID
    public long count;  //商品点击量
    public long windowStart;  //窗口开始时间
    public long windowEnd;  //窗口结束时间

    public ItemEventCount(String productId, String eventId, String categoryId, long count, long windowStart, long windowEnd) {
        this.productId = productId;
        this.eventId = eventId;
        this.categoryId = categoryId;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public ItemEventCount() {
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ItemEventCount{" +
                "productId='" + productId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
