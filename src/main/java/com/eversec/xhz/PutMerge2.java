package com.eversec.xhz;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 常用的使用方式是使用 hadoop 来进行日志分析。
 * 虽然我们可以将各个小日志都复制到 HDFS 中，但通常，需要将小文件合并成一大巨大的文件(TB)。
 * 一种是先在本地文件系统中合并，然后再放入HDFS，这种办法会很消耗本地磁盘空间，
 * 比如现在有 10TB 的分散日志文件，在本地合并时，起码还需要额外的 10TB 空间来存储合并后的文件。
 * 还有一种是在往HDFS复制的过程中合并。由于 HDFS 没有直接的命令行实现该功能，
 * 所以需要用它的API通过其它方式组合实现了。JAVA的实现方式：
 * @author zhangp
 *
 */
public class PutMerge2 {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		
		Path inputDir = new Path(args[0]);
		Path hdfsFile = new Path(args[1]);
		
		FileStatus[] inputFiles = local.listStatus(inputDir);
		FSDataOutputStream out = hdfs.create(hdfsFile);
		
		for(int i = 0; i < inputFiles.length; i++){
			System.out.println(inputFiles[i].getPath().getName());
			FSDataInputStream in = local.open(inputFiles[i].getPath());
			byte buffer[] = new byte[256];
			int bytesRead = 0;
			System.out.println(bytesRead);
			while ((bytesRead = in.read(buffer)) > 0) {
				out.write(buffer,0,bytesRead);
			}
			in.close();
		}
		out.close();
	}
}
