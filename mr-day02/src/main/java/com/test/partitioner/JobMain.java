package com.test.partitioner;

import com.test.sort.FlowSortBean;
import com.test.sort.FlowSortMapper;
import com.test.sort.FlowSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author Gavin
 * @date 2021/1/24 18:16
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "sort");

        //二、设置Job对象的相关的信息，里面含有8个小步骤
        //1、设置输入的路径，让程序找到源文件的位置
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("E://naxue/data/flowsort_output/part-r-00000"));

        //2、设置Mapper类型，并设置k2 v2
        job.setMapperClass(FlowPartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //3 4 5 6 四个步骤，都是Shuffle阶段，现在使用默认的就可以了
        job.setPartitionerClass(FlowPartitioner.class);

        job.setNumReduceTasks(4);

        //7、设置Reducer类型，并设置k3 v3
        job.setReducerClass(FlowPartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //8、设置输出的路径，让程序的结果存放到某个地方去
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem fileSystem = FileSystem.getLocal(configuration);
        Path outputPath = new Path("E://naxue/data/flowpartition_output");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        TextOutputFormat.setOutputPath(job, outputPath);

        //三、等待程序完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
