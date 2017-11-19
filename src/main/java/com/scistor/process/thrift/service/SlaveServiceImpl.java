package com.scistor.process.thrift.service;

import com.scistor.process.controller.OperatorScheduler;
import com.scistor.process.pojo.SlaveLocation;
import com.scistor.process.utils.ZKOperator;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.ZooKeeper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Administrator on 2017/11/6.
 */
public class SlaveServiceImpl implements SlaveService.Iface, RunningConfig {

	private static final Logger LOG = Logger.getLogger(SlaveServiceImpl.class);

	@Override
	public void addOperators(List<Map<String, String>> operators, boolean contained) throws TException {
		try {
			OperatorScheduler.scheduler(operators, contained);
		}catch (Exception e){
			LOG.error(String.format("Add operators to current workflow capture an exception, slave:[%s]", getSlaveLocation().toJsonString()), e);
		}
	}

    @Override
    public void removeOperator(String operatorMainClass, boolean isConsumer) throws TException {
        if (!isConsumer) {
            OperatorScheduler.queueMap.remove(operatorMainClass);
            cancleOperatorThread(operatorMainClass, false);
        } else {
            cancleOperatorThread(operatorMainClass, true);
        }
        //在ZK上将其从正在运行算子路径中删除
        try {
            ZooKeeper zookeeper = ZKOperator.getZookeeperInstance();
            ZKOperator.deleteZNode(zookeeper, null, ZK_RUNNING_OPERATORS + "/" + operatorMainClass);
        } catch (Exception e) {
            LOG.error(String.format("remove operator:[%s] capture an exception", operatorMainClass), e);
        }
    }

    @Override
	public String addComponent(String componentName, ByteBuffer componentInfo) throws TException {
		FileOutputStream fos;
		try {
			File file = new File(COMPONENT_LOCATION + File.separator + componentName + ".jar");
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			fos = new FileOutputStream(file);
			fos.write(componentInfo.array());
			fos.flush();
			fos.close();
            URL[] urls = refreshUrls();
            //更新ClassLoader
            if(urls == null || urls.length==0){
                LOG.info("could not get urls new operator may be invalid...");
            }else{
                OperatorScheduler.classLoader = new URLClassLoader(urls,Thread.currentThread().getContextClassLoader());
            }
			return "success";
		} catch (Exception e) {
			LOG.error(String.format("Add component to slave[%s] capture an exception", getSlaveLocation().toJsonString()), e);
			return String.format("Add component to slave[%s] capture an exception[%s]", getSlaveLocation().toJsonString(), e);
		}
	}

	private static SlaveLocation getSlaveLocation() {
		SlaveLocation location = new SlaveLocation();
		try {
			location.setIp(InetAddress.getLocalHost().getHostAddress());
			location.setPort(Integer.parseInt(SystemConfig.getString("thrift_server_port")));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return location;
	}

	private URL[] refreshUrls(){
		File[] files = new File(COMPONENT_LOCATION).listFiles();
		URL[] urls = new URL[files.length];
		for(int i=0; i<files.length; i++){
			try {
				urls[i]=files[i].toURI().toURL();
			} catch (MalformedURLException e) {
				LOG.error("refreshUrls error", e);
				return null;
			}
		}
		return urls;
	}

    private String cancleOperatorThread(String operatorMainClass, boolean isConsumer){
        try {
            Thread[] threads = findAllThreads();
            String str = "";
            if (operatorMainClass.equals("main")) {
                LOG.info("The main thread cannot be terminated!");
            } else if (operatorMainClass.equals("Reference Handler")) {
                LOG.info("It belongs to system process and cannot be terminated!");
            } else if (operatorMainClass.equals("Finalizer")) {
                LOG.info("It belongs to system process and cannot be terminated!");
            } else if (operatorMainClass.equals("Signal Dispatcher")) {
                LOG.info("It belongs to system process and cannot be terminated!");
            } else if (operatorMainClass.equals("Attach Listener")) {
                LOG.info("It belongs to system process and cannot be terminated!");
            } else {
                int success = 0;
                for (int i = 0; i < threads.length; i++) {
                    if (threads[i].isAlive() && threads[i].getName().equals(operatorMainClass)) {
                        str = str + threads[i].getName();
                        threads[i].stop();
                        success += 1;
                        LOG.info("从线程任务结束!");
                    }
                }
                return str;
            }
        } catch (Exception e) {
            if (isConsumer) {
                LOG.error("cancle operator[%s]'s consumer thread error", e);
            } else {
                LOG.error("cancle operator[%s]'s producer thread error", e);
            }
        }
        return "";
    }

    public static Thread[] findAllThreads() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        ThreadGroup topGroup = group;
        while (null != group) {
            topGroup = group;
            group = group.getParent();
        }
        int estimatedSize = topGroup.activeCount() * 2;
        Thread[] slackList = new Thread[estimatedSize];
        int actualSize = topGroup.enumerate(slackList);
        Thread[] actualThreads = new Thread[actualSize];
        System.arraycopy(slackList, 0, actualThreads, 0, actualSize);
        return actualThreads;
    }

}
