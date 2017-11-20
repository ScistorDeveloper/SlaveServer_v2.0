package com.scistor.process.controller;

import com.scistor.process.operator.TransformInterface;
import com.scistor.process.record.Record;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class HandleNewOperator implements Runnable {

	private static final Logger LOG = Logger.getLogger(HandleNewOperator.class);
	private Map<String, String> operator;
	private ArrayBlockingQueue<Record> queue = null;

	public HandleNewOperator(Map<String, String> operator, ArrayBlockingQueue<Record> queue) {
		super();
		this.operator = operator;
		this.queue = queue;
	}

	@Override
	public void run() {
		TransformInterface entry = null;
		try {
			String mainClass = operator.get("mainclass");
			String taskType = operator.get("task_type");
			entry = (TransformInterface) OperatorScheduler.classLoader.loadClass(mainClass).newInstance();
			entry.init(operator, queue);
			if(taskType.equals("producer")) {
				entry.producer();
			}else if(taskType.equals("consumer")){
				entry.consumer();
			}
		}catch (Exception e){
			LOG.error(e.toString());
		} finally {
			if (null != entry) {
				entry.close();
			}
		}
	}

}
