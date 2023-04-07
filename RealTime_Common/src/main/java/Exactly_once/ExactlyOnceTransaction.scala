package Exactly_once
/*
①查询之前写入数据库的偏移量Offsets
②从offsets位置获取一个流
③自己的运算，得到结果
④在一个事务中把结果和偏移量写出到数据库
以wordcount(累加)为例
设计mysqZ中怎么存储数据，offsets
数据:
   粒度:个word是一行
  主键: word
  offset: groupId, topic , partitionid, offset
  粒度:一个组消费一个主题的一 个分区是- -行
  主键: (groupId, topic, partitionid)
*/

object ExactlyOnceTransaction {

}
