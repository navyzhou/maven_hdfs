package com.yc.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class HdfsTask01 {
	/**
	 * 运行的时候应该会报错，提示没有权限。第一中方式是修改目录的权限  hdfs dfs -chmod 777 /user/navy
	 * 第二种方式是在连接的时候指定用户为root
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		// 连接到hdfs
		FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.30.130:9000/"), new Configuration(),"root");
	
		// 指定要操作的文件路径
		Path file = new Path("/user/navy/yc.txt");
		
		// 判断该文件是否存在
		if (fs.exists(file)){
			System.out.println("该文件已经存在...");
		} else {
			// 创建文件
			FSDataOutputStream fdos = fs.create(file, new Progressable() {
				@Override
				public void progress() {
					// 执行进度显示
					System.out.print(">>");
				}
			}); // 文件系统数据输出流
			
			Scanner input = new Scanner(System.in);  // 扫描器对象
			System.out.print("请输入内容:");
			String word = input.nextLine();
			fdos.write(word.getBytes());  // 向文件系统中的文件中写入内容
			fdos.flush();
			fdos.close();
			input.close();
			fs.close();
			System.out.println("文件创建成功...");
		}
	} 
}
