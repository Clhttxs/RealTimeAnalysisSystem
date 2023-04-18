import org.apache.kafka.clients.producer.Producer;
import CommonUtils.KafkaClientUtil;

public class Test {

    @org.junit.Test
    public void test(){
        Producer<String, String> producer = KafkaClientUtil.producer;

        for(int i = 1;i<=10;i++){
            KafkaClientUtil.sendDataToKafka("exactly_one","这是第"+String.valueOf(i)+"条消息");
        }
        KafkaClientUtil.flush();
        producer.close();
    }
}