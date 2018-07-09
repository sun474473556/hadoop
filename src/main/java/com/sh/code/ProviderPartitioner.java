package com.sh.code;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.HashMap;
import java.util.Map;

public class ProviderPartitioner extends Partitioner<Text,Data> {

	static Map<String,Integer> providerMap = new HashMap<String,Integer>();

	static{
		providerMap.put("135", 1);
		providerMap.put("136", 1);
		providerMap.put("137", 1);
		providerMap.put("138", 1);
		providerMap.put("139", 1);
		providerMap.put("150", 2);
		providerMap.put("159", 2);
		providerMap.put("182", 3);
		providerMap.put("183", 3);
	}

	@Override
	public int getPartition(Text text, Data data, int i) {
		String telNo = text.toString();
		String sub_tel = telNo.substring(0, 3);
		Integer num = providerMap.get(sub_tel);
		if (num == null) {
			return 0;
		}
		return num;
	}
}
