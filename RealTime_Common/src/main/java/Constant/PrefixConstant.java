package Constant;

/**
 * RedisKey前缀常量
 */
public interface PrefixConstant {
    String dau_redis_Preffix="startlog:";
    String user_info_redis_preffix= "userinfo:";
    String order_info_redis_preffix= "orderinfo:";
    String order_detail_redis_preffix= "orderdetail:";
    //在项目的生成环境对流量进行压测，测试最大的延迟时间。可以设置redis中缓存存放的时间间隔
    Integer max_delay_time = 60 * 3 ;
}
