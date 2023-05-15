package CanalClient;

import CommonUtils.KafkaClientUtil;
import CommonUtils.RedisUtil;
import Constant.PrefixConstant;
import Constant.TopicConstant;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import redis.clients.jedis.Jedis;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * 1.订阅order_info和order_detail的insert操作，将信息发送到kafka
 * 2.订阅user_info的insert和update操作，将信息发送到redis
 *    在redis怎么存?
 *    当前的需求是，根据userId查询用户信息
 *    key:userinfo: userId
 *    value: string
 */
public class SaleDetailClient {

    private static Jedis jedis = RedisUtil.getJedis();

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
          /**
                1.先创建一个客户端对象CanalConnector
                (1)SocketAddress address: canal server的主机名和端口号。 参考canal.properties 中的
                                       canal.ip = hadoop103
                                       canal.port = 11111
                (2)String destination: 参考canal.properties中的canal.destinations = example
                                    可以写canal.destinations中的一个或N个，代表要连接的Mysql实例配置文件instance.properties所在的目录名
                (3)String username:  参考 instance.properties中的canal.user
                                  当前版本没有，使用null
                (4)String password:  参考 instance.properties中的canal.passwd
                                  当前版本没有，使用null
         */
        CanalConnector canalConnector =
                CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102",11111), "example", null, null);
        //2.使用客户端对象连接 Canal server端
        canalConnector.connect();
        // 3.订阅表--格式:库名.表名
        canalConnector.subscribe("realtime.*");
        //4.尝试拉取(消费)canal server已经拿到的数据
        while (true){
            //最多从 Canal Server中获取100条数据库变更message
            /**
             * 一条message包含多个entry
             * 一个entry包含一个sql
             * 一个sql对应至少一条数据的变更
             */
            Message message = canalConnector.get(100);
            if(message.getId()==-1){
                System.out.println("当前没有新数据产生，歇5s再干活....");
                //现在没有数据产生了，歇会
                Thread.sleep(5000);
                //继续去拉 ，开始下次循环
                continue;
            }
            //当拉取到数据时，进行解析在当前的需求中只要order_info表的insert操作
            // System.out.println(message);
            List<CanalEntry.Entry> entries = message.getEntries();
            for(CanalEntry.Entry entry :entries){
                //表名
                String tableName = entry.getHeader().getTableName();
                //对order_info表可能有多种操作，例如开关事务，也算写操作，只要insert
                CanalEntry.EntryType entryType = entry.getEntryType();
                /**
                 * CanalEntry.EntryType 是Canal客户端SDK中枚举类型，表示数据库变更事件的类型。根据Canal的官方文档和代码定义可以知道，CanalEntry.EntryType 有以下几种类型：
                 * ROWDATA：表示行数据事件，即数据库表中的一行数据发生了变更，包括 INSERT、UPDATE 和 DELETE 事件；
                 * TRANSACTIONBEGIN：表示事务开始事件，即需要处理的多个行数据事件都在一个事务中；
                 * TRANSACTIONEND：表示事务结束事件，即一段时间内发生的一系列操作被提交；
                 * HEADER：表示 Canal 消息头事件，包含了当前变更事件的基本信息，如 binlog 文件名、位点等；
                 * HEARTBEAT：表示心跳事件，用于保持 Canal Server 和客户端间的连接状态。
                 */
                if (entryType.equals(CanalEntry.EntryType.ROWDATA)){
                    //5.进行解析
                    parseData(entry.getStoreValue(),tableName);
                }
            }
        }
    }

    private static void parseData(ByteString storeValue,String tableName) throws InvalidProtocolBufferException {
        //反序列化为RowChange:代表一个sql反序列化后的N行变化
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
        //EventType:sql最具体的类型操作的关键字
        //根据表名和操作类型判断数据具体写到什么地方
        if(tableName.equals("order_info")&&rowChange.getEventType()==CanalEntry.EventType.INSERT){
            sendDate(rowChange,TopicConstant.ORDER_INFO,true);
        }
        else if(tableName.equals("order_detail")&&rowChange.getEventType()==CanalEntry.EventType.INSERT){
            sendDate(rowChange,TopicConstant.ORDER_DETAIL,true);
        }
        else if(tableName.equals("user_info")&&(rowChange.getEventType()==CanalEntry.EventType.INSERT||rowChange.getEventType()==CanalEntry.EventType.UPDATE)){
            sendDate(rowChange,null,false);
        }
        }

    private static void sendDate(CanalEntry.RowChange rowChange,String topic,boolean isSendKafka){
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        // 一个RowData代表一行
        for(CanalEntry.RowData rowData : rowDatasList){
            JSONObject jsonObject = new JSONObject();
            //获取一行中insert后的所有列
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            for (CanalEntry.Column column:afterColumnsList){
                jsonObject.put(column.getName(),column.getValue());
            }
            if(isSendKafka){
                //生产到kafka
                KafkaClientUtil.sendDataToKafka(topic,jsonObject.toJSONString());
                KafkaClientUtil.flush();
            }else {
                //写到Redis
                //jedis.set(PrefixConstant.user_info_redis_preffix+jsonObject.getString("id"),jsonObject.toJSONString());
                jedis.setex(PrefixConstant.user_info_redis_preffix+jsonObject.getString("id"),7*24*60*60,jsonObject.toJSONString());
            }
        }
        if(isSendKafka) System.out.println("Kafka成功写入"+rowDatasList.size()+"行数据...");
        else System.out.println("Redis成功写入"+rowDatasList.size()+"行数据...");
    }
}