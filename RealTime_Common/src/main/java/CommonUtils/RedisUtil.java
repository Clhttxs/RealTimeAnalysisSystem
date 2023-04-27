package CommonUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 返回连接Redis的client
 */
public class RedisUtil {
        public static  JedisPool jedisPool;
        static {
           jedisPool = getJedisPoolInstance();
        }

        /**
         * 获取RedisPool实例（单例）
         * @return RedisPool实例
         */
        public static JedisPool getJedisPoolInstance() {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(1000);           // 最大连接数
            poolConfig.setMaxIdle(32);              // 最大空闲连接数
            poolConfig.setMaxWaitMillis(100*1000);  // 最大等待时间
            poolConfig.setTestOnBorrow(true);       // 检查连接可用性, 确保获取的redis实例可用
            jedisPool = new JedisPool(poolConfig,PropertiesUtil.getProperty("redis.host"), Integer.parseInt(PropertiesUtil.getProperty("redis.port")));
            return jedisPool;
        }

        /**
         * 从连接池中获取一个 Jedis 实例（连接）
         * @return Jedis 实例
         */
        public static Jedis getJedis() {
            return jedisPool.getResource();
        }
}