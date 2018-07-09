package com.sh.code;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DCReducer extends Reducer<Text,Data,Text,Data> {

	@Override
	protected void reduce(Text key, Iterable<Data> values, Context context) throws IOException, InterruptedException {
		long up_sum=0;
		long down_sum=0;
		for(Data bean:values){
			up_sum+=bean.getUpPayLoad();
			down_sum+=bean.getDownPayLoad();
		}
		Data bean=new Data(key.toString(), up_sum, down_sum);
		context.write(key, bean);
	}
}
