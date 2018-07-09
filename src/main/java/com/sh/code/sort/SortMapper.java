package com.sh.code.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable> {
	private InfoBean k=new InfoBean();

	@Override
	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		//首先获取一行的数据
		String line = value.toString();
		//由于我们第一个MapReduce处理完之后toString方法是以\t为分割写的，因此我们这里分割数据应该用\t来分割
		String[] fields = line.split("\t");
		//获取帐号
		String account = fields[0];
		//获取收入的值
		double income = Double.parseDouble(fields[1]);
		//获取支出的值
		double outcome = Double.parseDouble(fields[2]);
		//给v赋值
		k.set(account, income, outcome);
		//输出，把Bean当作key它自己就具有排序功能，因此我们只需要Bean就可以了，value的值我们可以用NullWritable来代替
		context.write(k, NullWritable.get());
	}
}
