package com.scistor.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;

/**
 * Created by WANG Shenghua on 2017/11/18.
 */
public class Test {

    public static void main(String[] args) throws Exception {
        File file = new File("D:\\HS\\fulltext\\LXX1121017084218178061210290081660008020170811000048131100019702_138_1389999.html");
        BufferedReader br = new BufferedReader(new FileReader(file));
        Map<String,String> data = getMap(br);
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
