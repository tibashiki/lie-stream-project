CREATE TABLE tb_anchor_audience_count
(
    `id` String comment '数据唯一id',
    `anchor_id` String comment '主播id',
    `deviceId` String comment '用户ID',
    `eventId` String comment '事件ID',
    `os` String comment '系统名称',
    `province` String comment '省份',
    `channel` String comment '下载渠道',
    `deviceType` String comment '设备类型',
    `eventTime` DateTime64 comment '数据中所携带的时间',
    `date` String comment 'eventTime转成YYYYMM格式',
    `hour` String comment 'eventTime转成HH格式列席',
    `processTime` DateTime DEFAULT now()
)
    ENGINE = ReplacingMergeTree(processTime)
          PARTITION BY (date, hour)
          ORDER BY id;