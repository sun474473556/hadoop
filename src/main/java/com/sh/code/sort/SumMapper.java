package com.sh.code.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumMapper extends Mapper<LongWritable,Text,Text,InfoBean> {

	private Text k=new Text();
	private InfoBean v=new InfoBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields =line.split("\t");
		String account = fields[0];
		double income = Double.parseDouble(fields[1]);
		double outcome=Double.parseDouble(fields[2]);
		k.set(account);
		v.set(account,income,outcome);
		context.write(k,v);
	}
}
