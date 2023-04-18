import CommonUtils.*;
import RealTimeUtils.JDBCUtil;
import org.apache.kafka.clients.producer.Producer;


public class Test {

    @org.junit.Test
    public void test(){

        KafkaClientUtil.sendDataToKafka("exactly_one","来自客户端的消息1");
        KafkaClientUtil.flush();
    }
}