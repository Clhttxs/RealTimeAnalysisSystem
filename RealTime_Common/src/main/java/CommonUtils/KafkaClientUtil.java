package CommonUtils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * KafkaUtil:  返回Kakfa生产者，让调用者调用内置的方法，将数据发送到Kafka指定的主题中。
 * 业务轻度关联: 只给你个生产者，具体怎么生产，由调用者负责。
 * 			   每个业务生产的方式都不一样。
 *
 * 业务重度关联: 把生产者私有，只提供公有的生产方法，如何生产由工具类控制。
 * 			  每个业务生产的方式都一样。
 */
public class KafkaClientUtil {
    //构造一个单列的生产者
    public static Producer<String,String> producer;

    //静态代码块,在类加载时执行,只会执行一次
    static {
        producer=getProducer();
    }

    //私有化构造器，确保无法创建实例对象
    private KafkaClientUtil(){};

    //私有化方法,使用单例模式构造producer
    public static Producer<String,String> getProducer(){
        //提供producer的配置
        Properties properties = new Properties();
        //配置Producer的参数信息
        //必须配置集群地址，kv序列化器
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,PropertiesUtil.getProperty("kafka.broker.list"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getProperty("key.serializer"));
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,PropertiesUtil.getProperty("value.serializer"));
        /**额外配置一下producer精准一次写入broker
         * 配置一下幂等性参数,设置 acks,重试次数 retries，默认是 int 最大值，2147483647
         */
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        System.out.println(PropertiesUtil.getProperty("kafka.broker.list"));
        System.out.println(PropertiesUtil.getProperty("key.serializer"));
        System.out.println(PropertiesUtil.getProperty("value.serializer"));
        return new KafkaProducer<String, String>(properties);
    }

    //发送value到Kafka
    public static void sendDataToKafka(String topic,String value){
        //发送kv类型的数据
        //ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        //发送非kv类型的数据
        producer.send(new ProducerRecord<String,String>(topic,value));
    }

    //生产的数据先进入buffer,满足一定条件(缓冲区大小,时间间隔)才会发送消息
    public static void flush(){
        producer.flush();
    }
}