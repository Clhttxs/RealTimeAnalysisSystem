import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Producer;
import CommonUtils.KafkaClientUtil;

public class Test {

    @org.junit.Test
    public void test(){
        for (int i = 1; i <=3 ; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("消息","第"+i+"条消息");
            KafkaClientUtil.sendDataToKafka("test",jsonObject.toJSONString());
        }
        KafkaClientUtil.flush();
    }
}