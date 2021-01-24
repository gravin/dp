package com.test.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Gavin
 * @date 2021/1/24 20:26
 */
public class FlowPartitioner extends Partitioner<Text, NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int numPartitions) {
        String line = text.toString();
        if (StringUtils.startsWith(line, "135")) {
            return 0;
        } else if (StringUtils.startsWith(line, "136")) {
            return 1;
        } else if (StringUtils.startsWith(line, "137")) {
            return 2;
        } else {
            return 3;
        }
    }
}
