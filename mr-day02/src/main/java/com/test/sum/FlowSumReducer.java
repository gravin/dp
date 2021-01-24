package com.test.sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Gavin
 * @date 2021/1/24 18:02
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;
        for (FlowBean bean : values) {
            upFlow += bean.getUpFlow();
            downFlow += bean.getDownFlow();
            upCountFlow += bean.getUpCountFlow();
            downCountFlow += bean.getDownCountFlow();
        }
        FlowBean summarizedBean = new FlowBean();
        summarizedBean.setUpFlow(upFlow);
        summarizedBean.setDownFlow(downFlow);
        summarizedBean.setUpCountFlow(upCountFlow);
        summarizedBean.setDownCountFlow(downCountFlow);
        context.write(key, summarizedBean);
    }
}
