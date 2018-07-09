package com.sh.code.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public  class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean>{
	private Text k=new Text();
	@Override
	protected void reduce(InfoBean bean, Iterable<NullWritable> values, Context context)
		throws IOException, InterruptedException {
		String account = bean.getAccount();
		k.set(account);
		context.write(k, bean);
	}
}
