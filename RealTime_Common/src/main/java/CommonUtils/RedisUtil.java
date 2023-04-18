package CommonUtils;

import redis.clients.jedis.Jedis;

/**
 * 返回连接Redis的client
 */
public class RedisUtil {
    public static Jedis getJedis(){
        String host = PropertiesUtil.getProperty("redis.host");
        int port = Integer.parseInt(PropertiesUtil.getProperty("redis.port"));
        return new Jedis(host,port);
    }
}