package com.yc.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTask02 {
	public static void main(String[] args) throws IOException {
		// 连接到hdfs
		FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.30.130:9000/"), new Configuration());
	
		// 指定要操作的文件路径
		Path file = new Path("/user/navy/yc.txt");
		
		// 判断该文件是否存在
		if (fs.exists(file)){
			FSDataOutputStream fdos = fs.append(file);
			fdos.write("衡阳市源辰信息科技有限公司".getBytes());
			fdos.flush();
			fdos.close();
			System.out.println("数据写入成功...");
		} 
		fs.close();
	} 
}
