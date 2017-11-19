package com.scistor.process.utils.params;

public interface RunningConfig {

	String COMPONENT_LOCATION = SystemConfig.getString("compenent_location");

	String ZOOKEEPER_ADDR = SystemConfig.getString("zookeeper_addr");

	String ZK_COMPONENT_LOCATION = SystemConfig.getString("zk_compenent_location");

	String ZK_RUNNING_OPERATORS = SystemConfig.getString("zk_running_operators");

	Integer ZK_SESSION_TIMEOUT = 365*24*60*60*1000;

	Integer SLAVES_SELECTOR_THREADS = Integer.parseInt(SystemConfig.getString("slaves_selector_threads"));

	Integer SLAVES_THREAD_POOL_SIZE = Integer.parseInt(SystemConfig.getString("slaves_thread_pool_size"));

}
