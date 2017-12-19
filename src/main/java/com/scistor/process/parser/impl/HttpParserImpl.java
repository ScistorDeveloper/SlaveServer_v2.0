package com.scistor.process.parser.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.scistor.process.controller.OperatorScheduler;
import com.scistor.process.parser.IParser;
import com.scistor.process.record.Record;
import com.scistor.process.record.extend.HttpRecord;
import com.scistor.process.utils.ZKOperator;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by WANG Shenghua on 2017/11/17.
 */
public class HttpParserImpl implements IParser {

    private static final Logger LOG =  Logger.getLogger(HttpParserImpl.class);
    private static final String ROOTDIR = "D:\\HS\\fulltext";//D:\HS\fulltext,/home/hadoop/apps/HS/fulltext
    private static List<String> handledDirs = new ArrayList<String>();

    @Override
    public void process() {
        //实时监控太极全文检索目录，解析其中的文件
        try {
            monitorRootDir();
        } catch (Exception e) {
            LOG.error("HTTP parse error", e);
        }
    }

    private void monitorRootDir() throws Exception {
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        Integer port = Integer.parseInt(SystemConfig.getString("thrift_server_port"));
        String zooKeeperDirsPath = SystemConfig.getString("zk_dirs_path");
        String NET = hostAddress + ":" + port;
        while(true) {
            LOG.debug("updating...");
            File rootFileDir = new File(ROOTDIR);
            ZooKeeper zooKeeper = ZKOperator.getZookeeperInstance();
            final String dayDirName = new String(zooKeeper.getData(zooKeeperDirsPath + "/" + NET, false, null));
            File[] dayFileDirs = rootFileDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return Integer.parseInt(name) > Integer.parseInt(dayDirName);
                }
            });
            List<File> dayFileList = Arrays.asList(dayFileDirs);
            List<File> fileList = new ArrayList<File>();
            for (File dir : dayFileList) {
                getFileList(dir, fileList);
                for (File zipFile : fileList) {
                    parse(zipFile);
                }
                //更新zookeeper上最近处理的文件
                zooKeeper.setData(zooKeeperDirsPath + "/" + NET, dir.getName().getBytes(), -1);
            }
            zooKeeper.close();
            Thread.sleep(24 * 60 * 60 * 1000);
        }
    }

    public void parse(File file) throws Exception {
        LOG.debug("parsing....");
        ZipFile zipFile = new ZipFile(file, Charset.forName("GBK"));
        Enumeration entries = zipFile.entries();
        while(entries.hasMoreElements()) {
            ZipEntry zipEntry = (ZipEntry) entries.nextElement();
            if (zipEntry.getName().endsWith(".txt") || zipEntry.getName().endsWith(".html")) {
                InputStream is = zipFile.getInputStream(zipEntry);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
                //将数据解析成为Map格式
                Map<String,String> dataMap = getMap(bufferedReader);
                if (dataMap.isEmpty()) {
                    bufferedReader.close();
                    is.close();
                    continue;
                }
                //放入数据队列，算子使用
                Record record = new HttpRecord(dataMap);
                ConcurrentHashMap<String, ArrayBlockingQueue<Record>> queueMap = OperatorScheduler.queueMap;
                Set<Map.Entry<String, ArrayBlockingQueue<Record>>> entrySet = queueMap.entrySet();
                for (Map.Entry<String, ArrayBlockingQueue<Record>> entry : entrySet) {
                    ArrayBlockingQueue<Record> value = entry.getValue();
                    value.put(record);
                    LOG.debug(String.format("Operator:[%s]'s current ArrayBlockingQueue size is [%s]", entry.getKey(), value.size()));
                }
                bufferedReader.close();
                is.close();
            }
        }
        zipFile.close();
    }

    public void getFileList(File dir, List<File> filelist) {
        File[] files = dir.listFiles(); // 该文件目录下文件全部放入数组
        for (File file : files) {
            String fileName = file.getName();
            if (file.isDirectory()) {
                getFileList(file, filelist);
            } else {
                if (fileName.endsWith(".zip")) {
                    filelist.add(file);
                }
            }
        }
    }

    private static Map<String,String> getMap(BufferedReader br) throws Exception {
        String host = "";
        String url = "";
        Map<String,String> data = new HashMap<String, String>();
        List<String> lines = new ArrayList<String>();
        String line;
        while((line = br.readLine()) != null)
        {
            if(line.indexOf("Host") != -1){
                if(line.split(":").length == 2) {
                    host = line.split(":")[1].trim();
                }
                lines.add(line.trim());
            } else if (line.length() > 4 && ("GET".equals(line.substring(0, 3)) || "POST".equals(line.substring(0, 4)))) {
                url = line.split(" ")[1];
                lines.add(line.trim());
            } else{
                lines.add(line.trim());
            }
        }
        if(lines.size() > 0) {
            data.put("host", host);
            data.put("url", url);
            line = StringUtils.join(lines, "[[--]]");
            if (line.indexOf("GET") >= 0) {
                //GET 数据
                String json = lines.get(lines.size()-1).replace("[[:]]",":");
                data.put("action_type","GET");
                try {
                    data.put("url",lines.get(0).split(" ")[1]);
                    JSONObject object = JSON.parseObject(json);
                    JSONObject dataObjs = JSON.parseObject(object.getString("data"));
                    Iterator iterator = dataObjs.keySet().iterator();
                    String key;
                    String value;
                    while (iterator.hasNext()) {
                        key = (String) iterator.next();
                        value = dataObjs.getString(key);
                        data.put(key.toLowerCase(), value);
                    }
                }catch (Exception e){
                    //数据乱码等问题
                    data.clear();
                    return data;
                }
            } else {
                //POST 数据
                data.put("action_type","POST");
                JSONArray array = new JSONArray();
                for (String li : lines){
                    if(li.indexOf("[[:]]") >= 0 && li.split("\\[\\[:\\]\\]").length > 1){
                        JSONObject object = new JSONObject();
                        String key = li.split("\\[\\[:\\]\\]")[0].toLowerCase();
                        String value = li.split("\\[\\[:\\]\\]")[1];
                        object.put("key", key);
                        object.put("value", value);
                        array.add(object);
                        if (key.equals("username") || key.equals("password")) {
                            data.put(key, value);
                        }
                    }else if(li.indexOf(":") >= 0 && li.split(":").length > 1){
                        data.put(li.split(":")[0].toLowerCase(),li.split(":")[1]);
                    }
                }
                data.put("request_parameters", array.toJSONString());
            }
        }
        return data;
    }

}
