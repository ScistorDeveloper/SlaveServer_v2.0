package com.scistor.process.utils;

import com.google.common.base.Objects;
import com.scistor.process.pojo.SlaveLocation;
import com.scistor.process.utils.params.RunningConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 
 * @description  zookeeper 操作类
 * @author zhujiulong
 * @date 2016年7月21日 上午11:42:47
 *
 */
public class ZKOperator implements RunningConfig {

	private static final Log LOG = LogFactory.getLog(ZKOperator.class);

	public static ZooKeeper getZookeeperInstance() throws IOException {
		final CountDownLatch cdl = new CountDownLatch(1);
		ZooKeeper zookeeper = new ZooKeeper(ZOOKEEPER_ADDR, RunningConfig.ZK_SESSION_TIMEOUT, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if (event.getState() == Event.KeeperState.SyncConnected) {
						cdl.countDown();
					}
				}
			});
		return zookeeper;
	}

	public static boolean createZnode(final ZooKeeper zk, String path, String data, final CountDownLatch cdl) throws InterruptedException, KeeperException {
		if(Objects.equal(zk, null)){
			throw new IllegalArgumentException("ERROR:zookeeper instance is not available....");
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		StringBuffer buffer=new StringBuffer();
		String[] znodes = path.split("[/]");
		int i;
		String result;
		for(i=0;i<znodes.length-1;i++){
			if(StringUtils.isBlank(znodes[i])){
				continue;
			}
			buffer.append("/").append(znodes[i]);
			if(zk.exists(buffer.toString(), false) != null){
				LOG.info(String.format("znode[%s] has exists,create child...",buffer.toString()));
				continue;
			}else{
				result=zk.create(buffer.toString(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				LOG.info(String.format("create znode[%s],result[%s]",buffer.toString(),result));
			}
		}
		buffer.append("/").append(znodes[i]);
		if(zk.exists(buffer.toString(), false) == null){
			result = zk.create(buffer.toString(), (data == null ? "" : data).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			LOG.info(String.format("create znode[%s], result[%s]", buffer.toString(), result));
			buffer.setLength(0);
			if(StringUtils.isNotBlank(result)){
				return true;
			}
		}else{
			LOG.info(String.format("znode[%s] has exists",buffer.toString()));
		}
		return false;
	}

	public static void deleteZNode(final ZooKeeper zookeeper, final CountDownLatch cdl, String path) throws InterruptedException, KeeperException {
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("ERROR:zookeeper instance is not available....");
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		if(zookeeper.exists(path, false)!=null){
			zookeeper.delete(path, -1);
			LOG.info("delete znode:" + path);
		}else{
			LOG.info("znode not exist:" + path);
		}
	}

	public static boolean uploadOperatorInfo(final ZooKeeper zookeeper, final CountDownLatch cdl, String componentName, String mainClass) throws InterruptedException, KeeperException {
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is not available....,zookeeper=="+zookeeper);
		}
		if(StringUtils.isBlank(componentName)){
			throw new IllegalArgumentException("compenent name is empty....,compenentName=="+componentName);
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		String zpath = ZK_COMPONENT_LOCATION + "/" + componentName;
		if(zookeeper.exists(zpath, false) != null){
			LOG.info(String.format("znode:[%s] already exists in zookeeper, when upload operator[%s] info...", zpath, componentName));
			return false;
		}else{
			return ZKOperator.createZnode(zookeeper, zpath, mainClass, cdl);
		}
	}

	public static List<SlaveLocation> getLivingSlaves(ZooKeeper zookeeper) throws KeeperException, InterruptedException{
		LOG.info("Getting living slaves in zookeeper...");
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is not available....,zookeeper=="+zookeeper);
		}
		List<SlaveLocation> list = new ArrayList<SlaveLocation>();
		List<String> children = zookeeper.getChildren(ZK_LIVING_SLAVES, false);
		for(String child : children){
			SlaveLocation loc = new SlaveLocation();
			loc.setIp(child.split(":")[0]);
			loc.setPort(Integer.parseInt(child.split(":")[1]));
			list.add(loc);
		}
		return list;
	}

	public static void registerSlaveInfo(ZooKeeper zookeeper, CountDownLatch cdl, SlaveLocation location, String company) throws InterruptedException, KeeperException{
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is not available....,zookeeper==" + zookeeper);
		}
		if(Objects.equal(location, null)){
			throw new IllegalArgumentException("thrift location instance is not available....,location==" + location);
		}
		if(!Objects.equal(cdl, null)){
			cdl.await();
		}
		String address = location.getIp() + ":" + location.getPort();
		byte[] info = location.toJsonString().getBytes();
		String zpath = ZK_LIVING_SLAVES + "/" + company + "/" + address;
		if(zookeeper.exists(zpath, false) == null){
			zookeeper.create(zpath, info, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		}else{
			LOG.info(String.format("znode:[%s] is exist, will delete it before save slave thrift service location info", zpath));
			zookeeper.delete(zpath, -1);
			zookeeper.create(zpath,info, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
		}
		LOG.info(String.format("save slave info on zPath:[%s], info:[%s]", zpath, location.toJsonString()));
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		ZooKeeper zookeeper = ZKOperator.getZookeeperInstance();
		ZKOperator.createZnode(zookeeper, ZK_LIVING_SLAVES + "/other", "", null);
		zookeeper.close();
	}

}
