package com.scistor.process.operator.impl;

import com.alibaba.fastjson.JSONObject;
import com.scistor.process.operator.TransformInterface;
import com.scistor.process.record.Record;
import com.scistor.process.record.extend.HttpRecord;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

public class GeneratePasswordBookOperator implements TransformInterface, RunningConfig
{

    private static final Log LOG = LogFactory.getLog(GeneratePasswordBookOperator.class);
    private static boolean shutdown = true;
    private static final String PATH = "/PasswordBook";
    private static final Integer BATCH_SIZE = 1000;
    private Integer index = 1;
    private String zookeeper_addr;
    private String topic = "com.scistor.process.operator.impl.GeneratePasswordBookOperator";
    private String mainclass;
    private String task_type;
    private ArrayBlockingQueue<Record> queue;
    private String broker_list;
    private KafkaProducer producer;
    private ConsumerConnector consumer;
    private FileSystem fs;
    private Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    private Map<String, Set<Map<String, String>>> records = new HashMap<String, Set<Map<String, String>>>();

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
            shutdown = false;
            //初始化Hadoop连接
            Configuration conf = new Configuration();
            String hdfsURI = SystemConfig.getString("hdfsURI");
            conf.set("fs.defaultFS", hdfsURI);
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.setBoolean("dfs.support.append", true);
            //拿到一个文件系统操作的客户端实例对象
            try {
                fs = FileSystem.get(new URI(hdfsURI), conf, SystemConfig.getString("hadoop_user"));
            } catch (Exception e) {
                LOG.error("获取文件系统出现异常", e);
            }
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
                    String username = record.get("username");
                    String password = record.get("password");
                    if (!isBlank(host) && !isBlank(username) && !isBlank(password)) {
                        String line = host + "||" + username + "||" + password;
                        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), line);
                        producer.send(kafkaRecord).get();
                        LOG.info(String.format("一条数据[%s]已经写入Kafka, topic:[%s]", line, topic));
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

        while (true) {
            if (shutdown) {
                LOG.info("GeneratePasswordBookOperator SHUTDOWN!!!!!");
                consumer.shutdown();
                break;
            }
        }
    }

    @Override
    public void close() {
        if (null != consumer) {
            consumer.shutdown();
        }
        if (null != producer) {
            producer.close();
        }
    }

    class HanldMessageThread implements Runnable {

        private KafkaStream<byte[], byte[]> kafkaStream = null;

        public HanldMessageThread(KafkaStream<byte[], byte[]> kafkaStream) {
            super();
            this.kafkaStream = kafkaStream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            long start = System.currentTimeMillis();
            while (iterator.hasNext()) {
                String message = new String(iterator.next().message());
                LOG.info(String.format("已经在Kafka topic:[%s], 消费一条数据:[%s]", topic, message));
                String[] split = message.split("\\|\\|");
                String host = split[0].trim();
                String username = split[1].trim();
                String password = split[2].trim();
                Set<Map<String, String>> set = records.get(host);
                if (null == set) {
                    set = new HashSet<Map<String, String>>();
                    Map<String, String> record = new HashMap<String, String>();
                    record.put("username", username);
                    record.put("password", password);
                    set.add(record);
                    records.put(host, set);
                } else {
                    Map<String, String> record = new HashMap<String, String>();
                    record.put("username", username);
                    record.put("password", password);
                    set.add(record);
                    records.put(host, set);
                }
                //进行批量操作，节省资源
                long current = System.currentTimeMillis();
                if (current - start > 10000) {
                    writeToHDFS();
                    records.clear();
                    start = System.currentTimeMillis();
                }
            }
            System.out.println("end.......");
        }

    }

    public void writeToHDFS() {
        FSDataOutputStream output = null;
        try {
            Iterator<Map.Entry<String, Set<Map<String, String>>>> iterator = records.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Set<Map<String, String>>> next = iterator.next();
                String key = next.getKey();
                Set<Map<String, String>> values = next.getValue();
                if (!fs.exists(new Path(PATH, key))) {
                    fs.create(new Path(PATH, key)).close();
                }
                output = fs.append(new Path(PATH, key));
                for(Map<String, String> value : values) { // 写入数据
                    String username = value.get("username");
                    String password = value.get("password");
                    JSONObject object = new JSONObject();
                    object.put("username", username);
                    object.put("password", password);
                    output.write((object.toJSONString() + "\n").getBytes("UTF-8"));
                }
                output.flush();
                output.close();
            }
        } catch (Exception e) {
            LOG.error("写HDFS出现异常", e);
        } finally {
            try {
                output.close();
            } catch (IOException e) {
                LOG.error("HDFS关闭输出流异常", e);
            }
        }
    }

    private boolean isBlank(String str) {
        boolean flag = false;
        if (null == str || str.equals("")) {
            flag = true;
        }
        return flag;
    }

}
