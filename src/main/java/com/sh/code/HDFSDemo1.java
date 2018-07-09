package com.sh.code;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class HDFSDemo1 {
	public static void main(String[] args) throws Exception{
		testDelete();
	}

	public static void testUpload() throws Exception {
		InputStream in = new FileInputStream("/Users/sunhao/word");
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://0.0.0.0:9000"), new Configuration());
		OutputStream out = fileSystem.create(new Path("/word"));
		IOUtils.copyBytes(in, out, 4096, true);
	}

	public static void testInload() throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://0.0.0.0:9000"), new Configuration());
		InputStream in = fileSystem.open(new Path("/test.txt"));
		OutputStream out = new FileOutputStream("/Users/sunhao/test.txt");
		IOUtils.copyBytes(in,out,4096,true);
	}

	public static void testDelete() throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://0.0.0.0:9000"), new Configuration());
		fileSystem.delete(new Path("/sum"), true);
	}

	public static void testMkdir() throws Exception {
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://0.0.0.0:9000"), new Configuration());
		fileSystem.mkdirs(new Path("/test.txt"));
	}
}
