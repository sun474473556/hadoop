package com.sh.code.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class HbaseDemo {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		//127.0.0.1:16010 可查 一般是zk配置
		conf.set("hbase.zookeeper.quorum","10.23.153.2");
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor hcd = new HTableDescriptor(TableName.valueOf("peoples"));
		HColumnDescriptor hcd_info = new HColumnDescriptor("info");
		hcd_info.setMaxVersions(3);
		HColumnDescriptor hcd_data = new HColumnDescriptor("data");
		hcd.addFamily(hcd_data);
		hcd.addFamily(hcd_info);
		admin.createTable(hcd);
		admin.close();
	}
}
