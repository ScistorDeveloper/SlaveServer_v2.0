package com.scistor.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.scistor.process.operator.impl.WhiteListFilterOperator;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.lang.reflect.Field;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by WANG Shenghua on 2017/11/18.
 */
public class Test {

    public static void main(String[] args) throws Exception {
//        File file = new File("D:\\HS\\fulltext\\LXX1121017084218178061210290081660008020170811000048131100019702_138_1389999.html");
//        BufferedReader br = new BufferedReader(new FileReader(file));
//        Map<String,String> data = getMap(br);

//        Class clazz = null;
//        try {
//            clazz = Class.forName("com.scistor.process.operator.impl.WhiteListFilterOperator");
//            Object o = clazz.newInstance();
//            Field[] fields = clazz.getDeclaredFields();
//            for( Field field : fields ){
//                if (field.getName().equals("shutdown")) {
//                    field.setAccessible(true);
//                    field.setBoolean(o, false);
//                }
//            }
//            WhiteListFilterOperator w = new WhiteListFilterOperator();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        Configuration conf = new Configuration();
        String hdfsURI = SystemConfig.getString("hdfsURI");
        conf.set("fs.defaultFS", hdfsURI);
        //拿到一个文件系统操作的客户端实例对象
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(hdfsURI), conf, SystemConfig.getString("hadoop_user"));
        } catch (Exception e) {
            System.out.println(String.format("获取文件系统出现异常:[%s]", e));
        }

        FSDataOutputStream output = null;
        try {
            output = fs.create(new Path("/data", System.currentTimeMillis()+".log"));
            output.write("abcdefg".getBytes("UTF-8"));
            output.flush();
        } catch (Exception e) {
            System.out.println(String.format("写HDFS出现异常:[%s]", e));
        } finally {
            try {
                output.close();
            } catch (IOException e) {
                System.out.println(String.format("HDFS关闭输出流异常:[%s]", e));
            }
        }

    }

    private static Map<String,String> getMap(BufferedReader br) throws Exception {

        String host = "";
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
            }else{
                lines.add(line.trim());
            }
        }
        if(lines.size() > 0) {
            data.put("host", host);
            line = StringUtils.join(lines, "[[--]]");
            if (line.substring(0, 3).equals("GET")) {
                //GET data
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
                    System.out.println("数据乱码？没有返回JSON？ " + host);
                    return null;
                }
            } else if (line.substring(0, 4).equals("POST")){
                //POST DATA
                data.put("action_type","POST");
                for (String li : lines){
                    if(li.indexOf("[[:]]") >= 0 && li.split("\\[\\[:\\]\\]").length > 1){
                        data.put(li.split("\\[\\[:\\]\\]")[0].toLowerCase(), li.split("\\[\\[:\\]\\]")[1]);
                    }else if(li.indexOf(":") >= 0 && li.split(":").length > 1){
                        data.put(li.split(":")[0].toLowerCase(),li.split(":")[1]);
                    }
                }
            }
        }
        return data;
    }

}
