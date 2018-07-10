package com.sh.code.ReverseIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReverseIndex {
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(ReverseIndex.class);
		job.setMapperClass(ReverseIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		job.setCombinerClass(ReverseIndexCombiner.class);
		job.setReducerClass(ReverseIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
	}

	private static class ReverseIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String path = fileSplit.getPath().toString();
			String filePath = path.substring(path.length()-5);
			for (String word: words) {
				k.set(word + "->"+filePath);
				v.set("1");
				context.write(k,v);
			}
		}
	}

	private static class ReverseIndexCombiner extends Reducer<Text,Text,Text,Text> {

		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String line = key.toString();
			String[] wordAndPath = line.split("->");
			int sum = 0;
			for (Text t : values) {
				sum+=Integer.parseInt(t.toString());
			}
			k.set(wordAndPath[0]);
			v.set(wordAndPath[1] + "->" + sum);
			context.write(k,v);
		}
	}

	private static class ReverseIndexReducer extends Reducer<Text,Text,Text,Text> {

		private Text v = new Text();


		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder builder = new StringBuilder();
			for (Text text : values) {
				builder.append(text+"\t");
			}
			v.set(builder.toString());
			context.write(key,v);
		}
	}
}
