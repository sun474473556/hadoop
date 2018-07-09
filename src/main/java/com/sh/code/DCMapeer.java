package com.sh.code;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DCMapeer extends Mapper<LongWritable,Text,Text,Data> {

	Text text = null;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line=value.toString();
		String[] fields=line.split("\t");
		String telNo=fields[1];
		long up=Long.parseLong(fields[8]);
		long down=Long.parseLong(fields[9]);
		Data bean=new Data(telNo, up, down);
		text=new Text(telNo);
		context.write(text,bean);
	}
}
