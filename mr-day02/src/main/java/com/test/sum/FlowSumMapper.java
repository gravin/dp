package com.test.sum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Gavin
 * @date 2021/1/24 17:55
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] flow = value.toString().split("\t");
        String phoneNum = flow[1];

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(flow[6]));
        flowBean.setDownFlow(Integer.parseInt(flow[7]));
        flowBean.setUpCountFlow(Integer.parseInt(flow[8]));
        flowBean.setDownCountFlow(Integer.parseInt(flow[9]));

        context.write(new Text(phoneNum), flowBean);
    }
}
