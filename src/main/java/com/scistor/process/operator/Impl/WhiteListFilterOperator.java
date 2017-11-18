package com.scistor.process.operator.Impl;

import com.scistor.process.operator.TransformInterface;
import com.scistor.process.record.Record;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by WANG Shenghua on 2017/11/18.
 */
public class WhiteListFilterOperator implements TransformInterface {

    @Override
    public void init(Map<String, String> config, ArrayBlockingQueue<Record> queue) {

    }

    @Override
    public List<String> validate() {
        return null;
    }

    @Override
    public void producer() {

    }

    @Override
    public void consumer() {

    }

}
