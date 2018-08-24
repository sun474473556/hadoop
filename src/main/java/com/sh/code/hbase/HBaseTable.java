package com.sh.code.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTable {

	private Configuration conf = null;

	@Before
	public void init() {
		conf = HBaseConfiguration.create();
		//127.0.0.1:16010 可查 一般是zk配置
		conf.set("hbase.zookeeper.quorum","10.23.145.84");
	}

	@Test
	public void tetsPut() throws IOException {
		HTable table  = new HTable(conf,"peoples");
		Put put = new Put(Bytes.toBytes("rk0001"));
		put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("zhansan"));
		put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("20"));
		put.add(Bytes.toBytes("info"),Bytes.toBytes("money"),Bytes.toBytes("800000"));
		table.put(put);
		table.close();
	}

	@Test
	public void testPutAll() throws IOException {
		HTable table = new HTable(conf,"peoples");
		List<Put> puts=new ArrayList<Put>(10000);
		for(int i = 0;i<1000000;i++) {
			Put put = new Put(Bytes.toBytes("rk"+i));
			put.add(Bytes.toBytes("info"),Bytes.toBytes("money"),Bytes.toBytes(""+i));
			puts.add(put);
			if(i%10000==0){
				table.put(puts);
				puts=new ArrayList<Put>(10000);
			}
		}
		table.close();
	}

	@Test
	public void testGet() throws IOException {
		HTable table = new HTable(conf,"peoples");
		Get get = new Get(Bytes.toBytes("rk99999"));
		Result result = table.get(get);
		String r = Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("money")));
		System.out.println(r);
		table.close();
	}

	@Test
	public void testScan() throws IOException{
		//要想向表中插入数据，我们当然要先得到这张表了，通过下面这句代码可以获取我们要操作的表
		HTable table=new HTable(conf, "peoples");
		//浏览rk从29990到rk300000，一会大家看看是不是跟你想象的结果一样。
		Scan scan=new Scan(Bytes.toBytes("rk29990"),Bytes.toBytes("rk300000"));
		ResultScanner scanner=table.getScanner(scan);
		for(Result result:scanner){
			String r=Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money")));
			System.out.println(r);
		}
		table.close();
	}

	@Test
	public void testDelete() throws IOException{
		//要想向表中插入数据，我们当然要先得到这张表了，通过下面这句代码可以获取我们要操作的表
		HTable table=new HTable(conf, "peoples");
		//指定要删除哪一行的数据
		Delete delete=new Delete(Bytes.toBytes("rk99999"));
		//执行删除
		table.delete(delete);
		//删除完关闭table
		table.close();
	}


}
