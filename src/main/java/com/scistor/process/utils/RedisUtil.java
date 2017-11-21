package com.scistor.process.utils;

import com.scistor.process.operator.impl.WhiteListFilterOperator;
import com.scistor.process.pojo.DataInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.*;

/**
 * Created by WANG Shenghua on 2017/10/27.
 */
public class RedisUtil {

    private static final Log LOG = LogFactory.getLog(RedisUtil.class);
    private static JedisCluster jedisCluster = null;
    private static final String KEY1 = "COUNT";
    private static final String KEY2 = "STATUS";

    static {
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7000));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7001));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7002));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7003));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7004));
        jedisClusterNodes.add(new HostAndPort("172.16.18.234", 7005));
        jedisCluster = new JedisCluster(jedisClusterNodes);
    }

    public static TreeSet<String> getRedisKeys () {
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        TreeSet<String> keys = new TreeSet<String>();
        for(String k : clusterNodes.keySet()){
            LOG.debug("Getting keys from: " + k);
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys("*"));
            } catch(Exception e){
                LOG.error("Getting keys error: {}", e);
            } finally{
                LOG.debug("Connection closed.");
                connection.close();
            }
        }
        return keys;
    }

    public static  Map<String, String> getHost(String host) {
        return jedisCluster.hgetAll(host);
    }

    public static void put(String host, Map<String,String> map) {
        jedisCluster.hmset(host,map);
    }

    public static List<DataInfo> SortedHostByCount(TreeSet<String> hosts) {
        int totalCount = 0;
        List<DataInfo> dataInfos = new ArrayList<DataInfo>();
        Iterator<String> it = hosts.iterator();
        while (it.hasNext()) {
            String host = it.next();
            List<String> hmget = jedisCluster.hmget(host, KEY1, KEY2);
            if (null != hmget && hmget.size() > 0) {
                dataInfos.add(new DataInfo(host, Integer.parseInt(hmget.get(0)), hmget.get(1)));
                totalCount = totalCount + Integer.parseInt(hmget.get(0));
            }
        }
        Collections.sort(dataInfos, new Comparator<DataInfo>() {
            public int compare(DataInfo o1, DataInfo o2) {
                int count1 = o1.getCOUNT();
                int count2 = o2.getCOUNT();
                if (count1 > count2) {
                    return -1;
                } else if (count1 < count2) {
                    return  1;
                } else  {
                    return o1.getHOST().compareTo(o2.getHOST());
                }
            }
        });
        System.out.println(totalCount);
        return  dataInfos;
    }

}
