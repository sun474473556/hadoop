package com.sh.code.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumReducer extends Reducer<Text,InfoBean,Text,InfoBean> {
	private InfoBean v=new InfoBean();

	@Override
	protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
		double sum_income=0;
		double sum_outcome=0;
		for(InfoBean bean:values){
			sum_income+=bean.getIncome();
			sum_outcome+=bean.getOutcome();
		}
		v.set("", sum_income, sum_outcome);
		context.write(key, v);
	}
}
