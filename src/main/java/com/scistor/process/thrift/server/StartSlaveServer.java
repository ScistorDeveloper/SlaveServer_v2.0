package com.scistor.process.thrift.server;

import com.scistor.process.pojo.SlaveLocation;
import com.scistor.process.thrift.service.SlaveService;
import com.scistor.process.thrift.service.SlaveServiceImpl;
import com.scistor.process.utils.ZKOperator;
import com.scistor.process.utils.params.RunningConfig;
import com.scistor.process.utils.params.SystemConfig;
import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.ZooKeeper;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/11/6.
 */
public class StartSlaveServer implements RunningConfig{

    private static final Logger LOG =  Logger.getLogger(StartSlaveServer.class);
    private static String hostAddress;
    private static int PORT = Integer.parseInt(SystemConfig.getString("thrift_server_port"));
    private static String company;

    public static void main(String[] args) throws NumberFormatException, TTransportException {
        if (args.length < 1) {
            LOG.error("Please specify the company name which this machine belongs to");
            return;
        }
        company = args[0];
        start(args);
    }

    public static void start(String[] args) throws NumberFormatException, TTransportException{
        TProcessor processor = new SlaveService.Processor<SlaveService.Iface>(new SlaveServiceImpl());
        TNonblockingServerSocket serverSocket=new TNonblockingServerSocket(PORT);
        LOG.info("thrift transport init finished...");

        TThreadedSelectorServer.Args m_args=new TThreadedSelectorServer.Args(serverSocket);

        m_args.processor(processor);
        m_args.processorFactory(new TProcessorFactory(processor));
        m_args.protocolFactory(new TCompactProtocol.Factory());
        m_args.transportFactory(new TFramedTransport.Factory());
        m_args.selectorThreads(SLAVES_SELECTOR_THREADS);

        ExecutorService threads = Executors.newFixedThreadPool(SLAVES_THREAD_POOL_SIZE);
        m_args.executorService(threads);
        LOG.info("thrift nio channel selector  init finished...");
        TThreadedSelectorServer server=new TThreadedSelectorServer(m_args);
        LOG.info("server starting...");

        try {
            ZooKeeper zookeeper = ZKOperator.getZookeeperInstance();
            SlaveLocation loc = new SlaveLocation();
            hostAddress = InetAddress.getLocalHost().getHostAddress();
            loc.setIp(hostAddress);
            loc.setPort(PORT);
            ZKOperator.registerSlaveInfo(zookeeper, null, loc, company);
        } catch (Exception e) {
            LOG.error(e);
        }

        server.serve();
    }


}
