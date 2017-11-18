package com.scistor.process.thrift.service;

import com.scistor.process.controller.OperatorScheduler;
import com.scistor.process.pojo.SlaveLocation;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Administrator on 2017/11/6.
 */
public class SlaveServiceImpl implements SlaveService.Iface, RunningConfig {

	private static final Logger LOG = Logger.getLogger(SlaveServiceImpl.class);

	@Override
	public String addNewOperators(List<Map<String, String>> operators, boolean contained) throws TException {
		try {
			OperatorScheduler.scheduler(operators, contained);
			return "success";
		}catch (Exception e){
			LOG.error(String.format("Add operators to current workflow capture an exception, slave:[%s]", getSlaveLocation().toJsonString()), e);
			return String.format("Add operators to current workflow capture an exception:[%s], slave:[%s]", e.getMessage(), getSlaveLocation().toJsonString());
		}
	}

	@Override
	public String addNewComponent(String componentName, ByteBuffer componentInfo) throws TException {
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
			return "success";
		} catch (Exception e) {
			LOG.error(String.format("Add component to slave[%s] capture an exception", getSlaveLocation().toJsonString()), e);
			return(String.format("Add component to slave[%s] capture an exception[%s]", getSlaveLocation().toJsonString(), e));
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

}
