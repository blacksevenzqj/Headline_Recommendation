#!/usr/bin/env bash
#
## user_profile
#sqoop import \
#        --connect jdbc:mysql://192.168.19.137/toutiao \
#        --username root \
#        --password password \
#        --table user_profile \
#        --m 4 \
#        --target-dir /user/hive/warehouse/toutiao.db/user_profile \
#        --incremental lastmodified \
#        --check-column update_time \    # Mysql的更新时间字段
#        --merge-key user_id \   # Mysql的主键
#        --last-value "2018-01-01 00:00:00"



# 一、增量导入：导入到HDFS中（手动创建HIVE表与之关联）
# 1、用户表：user_profile、user_basic；2、文章表：news_channel
time=`date +"%Y-%m-%d" -d "-1day"`
declare -A check
check=([user_profile]=update_time [user_basic]=last_login [news_channel]=update_time)
declare -A merge
merge=([user_profile]=user_id [user_basic]=user_id [news_channel]=channel_id)

for k in ${!check[@]}
do
    sqoop import \
        --connect jdbc:mysql://192.168.19.137/toutiao \
        --username root \
        --password password \
        --table $k \
        --m 4 \
        --target-dir /user/hive/warehouse/toutiao.db/$k \
        --incremental lastmodified \
        --check-column ${check[$k]} \
        --merge-key ${merge[$k]} \
        --last-value ${time}
done


# 2、文章表：news_article_basic 注意特殊字符处理，所以单独拉出来
sqoop import \
    --connect jdbc:mysql://192.168.19.137/toutiao?tinyInt1isBit=false \
    --username root \
    --password password \
    --m 4 \
    --query 'select article_id, user_id, channel_id, REPLACE(REPLACE(REPLACE(title, CHAR(13),""),CHAR(10),""), ",", " ") title, status, update_time from news_article_basic WHERE $CONDITIONS' \
    --split-by user_id \
    --target-dir /user/hive/warehouse/toutiao.db/news_article_basic \
    --incremental lastmodified \
    --check-column update_time \
    --merge-key article_id \
    --last-value ${time}



# 二、全量导入：直接导入到Hive表中（不需要手动创建HIVE表，会自动创建HIVE表）
# 1、文章表：news_article_content，由于news_article_content文章内容表中含有过多特殊字符，选择直接全量导入
sqoop import \
    --connect jdbc:mysql://192.168.19.137/toutiao \
    --username root \
    --password password \
    --table news_article_content \
    --m 4 \
    --hive-home /root/bigdata/hive \
    --hive-import \
    --hive-drop-import-delims \
    --hive-table toutiao.news_article_content \
    --hive-overwrite