package com.scistor.process.controller;

import com.scistor.process.operator.impl.WhiteListFilterOperator;
import com.scistor.process.parser.IParser;
import com.scistor.process.record.Record;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by WANG Shenghua on 2017/11/18.
 */
public class OperatorScheduler implements RunningConfig{

    private static final Log LOG = LogFactory.getLog(OperatorScheduler.class);
    public static ConcurrentHashMap<String, ArrayBlockingQueue<Record>> queueMap = new ConcurrentHashMap<String, ArrayBlockingQueue<Record>>();
    private static int QUEUE_SIZE;
    public static ClassLoader classLoader = null;

    static {
        //初始化类加载器
        initClassLoader();

        //为系统中的算子初始化消息队列，当系统刚启动时，只有一个内置的白名单过滤算子
        QUEUE_SIZE = Integer.parseInt(SystemConfig.getString("queue_size"));

        //只有部署在太极集群上的从节点要进行数据解析，部署在赛思的从节点只处理汇聚操作
        String company = SystemConfig.getString("slave_server_location");
        if(company.equals("other")) {
            //读配置文件，决定初始化哪一个数据解析算子
            String parseMainClass = SystemConfig.getString("data_parser");
            try {
                final IParser parserEntity = (IParser) classLoader.loadClass(parseMainClass).newInstance();
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        parserEntity.process();
                    }
                });
            } catch (Exception e) {
                LOG.error("数据解析算子加载异常", e);
            }
        }

    }

    public static void scheduler(List<Map<String, String>> operators, boolean consumer) {

        if(!consumer) {
            initArrayBlockingQueueForNewOperators(operators);
        }

        Thread[] threads = new Thread[operators.size()];
        int i = 0;
        for (Map<String, String> operator : operators) {
            if (!consumer) {
                threads[i] = new Thread(new HandleNewOperator(operator, queueMap.get(operator.get("mainclass"))));
            } else {
                threads[i] = new Thread(new HandleNewOperator(operator, null));
            }
            threads[i].setName(operator.get("mainclass"));
            threads[i].start();
        }

    }

    private static void initClassLoader() {
        File compenents = new File(RunningConfig.COMPONENT_LOCATION);
        File[] files = compenents.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        });
        URL[] urls = new URL[files == null || files.length == 0 ? 0 : files.length];
        for(int i = 0; i < (files == null ? 0 : files.length); i++){
            try {
                urls[i] = files[i].toURI().toURL();
            } catch (MalformedURLException e) {
                LOG.error(e);
            }
        }
        if(urls == null || urls.length == 0){
            classLoader = Thread.currentThread().getContextClassLoader();
        }else{
            classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        }
    }

    private static void initArrayBlockingQueueForNewOperators(List<Map<String, String>> operators) {
        for (Map<String, String> operator : operators) {
            String mainClass = operator.get("mainclass");
            queueMap.put(mainClass, new ArrayBlockingQueue<Record>(QUEUE_SIZE));
        }
    }

}
