package com.lf.bigdata.spark

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.Err

/**
 * @Classname SparkStreamingKafkaDemo
 * @Date 2021/9/13 8:14 上午
 * @Created by LiuFei
 */
object SparkStreamingKafkaDemo {

  // 读取 offset（从kafka读取offset）
  def readOffset(kafkaCluster: KafkaCluster, topic: String, group: String) : Map[TopicAndPartition, Long] = {
    // 最终返回的分区及offset信息
    var result: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()

    // 获取指定topic的分区信息的Either
    val topicAndPartition: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    if (topicAndPartition.isRight) {
      // 获取分区信息
      val topicAndPartitions: Set[TopicAndPartition] = topicAndPartition.right.get
      // 获取分区及offset信息的Either
      val topicAndPartitionsOffsets: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitions)

      // 如果没有分区及offset信息，第一次消费
      if (topicAndPartitionsOffsets.isLeft) {

        topicAndPartitions.foreach(topicAndPartition => {
          result += topicAndPartition -> 0L
        })
      } else { // 有分区及offset信息，则返回最新分区及offset信息
        result = topicAndPartitionsOffsets.right.get
      }
    }

    result
  }

  // 保存offset
  def saveOffset(kafkaCluster: KafkaCluster, group: String, topic: String, dstream: InputDStream[String]) = {
    dstream.foreachRDD(rdd => {
      var topicAndPartitionMap: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      // 每个分区的offset
      val ranges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
      ranges.foreach(offsetRange => {
        topicAndPartitionMap += offsetRange.topicAndPartition() -> offsetRange.untilOffset
      })
      kafkaCluster.setConsumerOffsets(group, topicAndPartitionMap)
    })
  }

  // 主函数
  def main(args: Array[String]): Unit = {
    // kafka 参数
    val brokers: String = "a-lf-bigdata:6667,b-lf-bigdata:6667,c-lf-bigdata:6667"
    val topic: String = "test"
    val group: String = "test_group1"

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingKafkaDemo")
    var params: Map[String, String] = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG-> group)

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val kafkaCluster: KafkaCluster = new KafkaCluster(params)

    // 读取上次消费的offset位置
    val fromOffsets: Map[TopicAndPartition, Long] = readOffset(kafkaCluster, topic, group)

    val dstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      params,
      fromOffsets,
      (message: MessageAndMetadata[String, String]) => message.message()
    )

    // 处理业务
    dstream.print(100)

    // 保存offset
    saveOffset(kafkaCluster, group, topic, dstream)

    ssc.start()
    ssc.awaitTermination()
  }
}
