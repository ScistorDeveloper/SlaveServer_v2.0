package com.scistor.process.operator.impl;

import com.scistor.process.operator.TransformInterface;
import com.scistor.process.record.Record;
import com.scistor.process.record.extend.HttpRecord;
import com.scistor.process.utils.Map2String;
import com.scistor.process.utils.TopicUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by WANG Shenghua on 2017/11/18.
 */
public class WhiteListFilterOperator implements TransformInterface {

    private static final Log LOG = LogFactory.getLog(WhiteListFilterOperator.class);
    private String zookeeper_addr;
    private String topic = "com.scistor.process.operator.impl.WhiteListFilterOperator";
    private String mainclass;
    private String task_type;
    private ArrayBlockingQueue<Record> queue;
    private String broker_list;
    private KafkaProducer producer;
    private ConsumerConnector consumer;
    private Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

    @Override
    public void init(Map<String, String> config, ArrayBlockingQueue<Record> queue) {

        this.zookeeper_addr = config.get("zookeeper_addr");
        this.broker_list = config.get("broker_list");
        this.task_type = config.get("task_type");
        this.mainclass = config.get("mainclass");
        this.queue = queue;

        Properties props = new Properties();
        if (task_type.equals("producer")) {
            props.put("producer.type","sync");
            props.put("bootstrap.servers", broker_list);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("request.required.acks", "1");
            producer = new KafkaProducer<String, String>(props);
        } else if (task_type.equals("consumer")) {
            props.put("zookeeper.connect", zookeeper_addr);
            props.put("auto.offset.reset","smallest");
            props.put("group.id", "HS");
            props.put("zookeeper.session.timeout.ms", "86400000");
            props.put("zookeeper.sync.time.ms", "5000");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "5000");
            topicCountMap.put(topic, 1);
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        }

    }

    @Override
    public List<String> validate() {
        return null;
    }

    @Override
    public void producer() {
        try {
            while(true){
                System.out.println("producing...");
                if(queue.size() > 0) {
                    Map<String, String> record = ((HttpRecord)queue.take()).getRecord();
                    String host = record.get("host");
                    if (null != host && !"".equals(host)) {
                        String line = Map2String.transMapToString(record);
                        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), host+"|| "+line);
                        producer.send(kafkaRecord).get();
                        LOG.info(String.format("一条数据[%s]已经写入Kafka, topic:[%s]", host+"|| "+line, topic));
                    }
                }else {
                    System.out.println("waiting...");
                    Thread.sleep(1000);
                }
            }
        }
        catch (Exception e){
            LOG.error(String.format("Operator:[%s]'s producer part capture an Exception", mainclass), e);
        } finally {
            producer.close();
        }
    }

    @Override
    public void consumer() {

        Map<String, List<KafkaStream<byte[], byte[]>>> msgStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> msgStreamList = msgStreams.get(topic);

        Thread[] threads = new Thread[msgStreamList.size()];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new HanldMessageThread(msgStreamList.get(i)));
            threads[i].setName(mainclass);
            threads[i].start();
        }

        LOG.info(String.format("Number of thread is [%s]", threads.length));

    }

    class HanldMessageThread implements Runnable {

        private KafkaStream<byte[], byte[]> kafkaStream = null;

        public HanldMessageThread(KafkaStream<byte[], byte[]> kafkaStream) {
            super();
            this.kafkaStream = kafkaStream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            while (iterator.hasNext()) {
                String message = new String(iterator.next().message());
                LOG.info(String.format("已经在Kafka topic:[%s], 消费一条数据:[%s]", topic, message));
            }
        }

    }

}
