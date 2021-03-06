package com.scistor.process.operator.impl;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.scistor.process.operator.TransformInterface;
import com.scistor.process.record.Record;
import com.scistor.process.record.extend.HttpRecord;
import com.scistor.process.utils.Map2String;
import com.scistor.process.utils.RedisUtil;
import com.scistor.process.utils.TopicUtil;
import com.scistor.process.utils.params.SystemConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by WANG Shenghua on 2017/11/18.
 */
public class WhiteListFilterOperator implements TransformInterface {

    private static final Log LOG = LogFactory.getLog(WhiteListFilterOperator.class);
    private static boolean shutdown = true;
    private static final String KEY1 = "COUNT";
    private static final String KEY2 = "STATUS";
    private static final String PATH = "/data";
    private static final Integer BATCH_SIZE = 200000;
    private static final Integer UPDATE_TIME = 60000;
    private Integer index = 1;
    private String zookeeper_addr;
    private String topic = "com.scistor.process.operator.impl.WhiteListFilterOperator";
    private String mainclass;
    private String task_type;
    private ArrayBlockingQueue<Record> queue;
    private String broker_list;
    private KafkaProducer producer;
    private ConsumerConnector consumer;
    private  Configuration conf;
    private FileSystem fs;
    private Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    private Map<String, Integer> hostCount = new HashedMap();
    private List<String> messages = new ArrayList<String>();
    private static BloomFilter<CharSequence> bloomFilter;

    static {
        updateBloomFilter();
    }

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
            conf = new Configuration();
            String hdfsURI = SystemConfig.getString("hdfsURI");
            conf.set("fs.defaultFS", hdfsURI);
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
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
            long start = System.currentTimeMillis();
            while(true){
                long end = System.currentTimeMillis();
                if (end - start > UPDATE_TIME) {
                    updateBloomFilter();
                    start = System.currentTimeMillis();
                }
                LOG.debug("producing...");
                if(queue.size() > 0) {
                    Map<String, String> record = ((HttpRecord)queue.take()).getRecord();
                    String host = record.get("host");
                    if (null != host && !"".equals(host)) {
                        boolean contained = isHostInWhiteList(host);
                        if (!contained) {
                            //不在白名单中的发送到kafka中
                            String line = Map2String.transMapToString(record);
                            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), host+"|| "+line);
                            producer.send(kafkaRecord).get();
                            LOG.debug(String.format("一条数据[%s]已经写入Kafka, topic:[%s]", host+"|| "+line, topic));
                        }
                    }
                }else {
                    LOG.debug("waiting...");
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
                LOG.info("WhiteListFilterOperator SHUTDOWN!!!!!");
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
            while (iterator.hasNext()) {
                String message = new String(iterator.next().message());
                messages.add(message);
                String host = message.split("\\|\\|")[0];
                if(hostCount.get(host) == null) {
                    hostCount.put(host, 1);
                } else {
                    hostCount.put(host, hostCount.get(host) + 1);
                }
                //进行批量操作，节省资源
                if (index >= BATCH_SIZE) {
                    updateRedis();
                    hostCount.clear();
                    writeToHDFS();
                    messages.clear();
                    index = 0;
                }
                index++;
                LOG.debug(String.format("已经在Kafka topic:[%s], 消费一条数据:[%s]", topic, message));
            }
        }

    }

    private void updateRedis() {
        Iterator<String> it = hostCount.keySet().iterator();
        while (it.hasNext()) {
            String host = it.next();
            Map<String, String> hostMap = RedisUtil.getHost(host);
            Map<String, String> map = new HashMap<String, String>();
            if (hostMap.size() == 0) {
                map.put(KEY1, "1");
                map.put(KEY2, "0");
                RedisUtil.put(host, map);
            } else {
                int newCount = Integer.parseInt(hostMap.get(KEY1)) + hostCount.get(host);
                String status = hostMap.get(KEY2);
                map.put(KEY1, newCount + "");
                map.put(KEY2, status);
                RedisUtil.put(host, map);
            }
        }
    }

    public void writeToHDFS() {
        SequenceFile.Writer writer = null;
        try {
            IntWritable key = new IntWritable();
            Text value = new Text();
            writer = SequenceFile.createWriter(fs, conf, new Path(PATH, System.currentTimeMillis()+".seq"), key.getClass(),value.getClass(), SequenceFile.CompressionType.RECORD);
            int i = 1;
            for(String message : messages) { // 写入数据
                key.set(i);
                value.set((message + "\n").getBytes("UTF-8"));
                writer.append(key, value);
                writer.hflush();
                i++;
            }
        } catch (Exception e) {
            LOG.error("写HDFS出现异常", e);
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    private boolean isHostInWhiteList(String host) {
        boolean mightContain = bloomFilter.mightContain(host);
        if (mightContain) {
            return true;
        } else {
            return false;
        }
    }

    private static void updateBloomFilter() {
        LOG.info(String.format("Start to update the BloomFilter!"));
        TreeSet<String> keySets = RedisUtil.getWhiteListHost();
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(), keySets.size() * 100, 0.0001F);
        Iterator<String> it = keySets.iterator();
        while (it.hasNext()) {
            bloomFilter.put(it.next());
        }
        LOG.info(String.format("The BloomFilter has been updated!"));
    }

}
