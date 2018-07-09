package com.sh.code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DataCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		//mapper
		job.setMapperClass(DCMapeer.class);
		job.setJarByClass(DataCount.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Data.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));

		//分区
		job.setPartitionerClass(ProviderPartitioner.class);

		//map
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Data.class);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		//设置多个reduce，一个reduce对应一个文件
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.waitForCompletion(true);

	}
}
