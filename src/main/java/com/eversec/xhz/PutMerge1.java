package com.eversec.xhz;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

//参数1为本地目录，参数2位HDFS上的文件
public class PutMerge1 {
	public static void putMergeFunc(String LocalDir,String fsFile) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path localDir = new Path(LocalDir);
		Path HDFSFile = new Path(fsFile);
		
		FileStatus[] status = fs.listStatus(localDir);//得到输入目录
		FSDataOutputStream out = fs.create(HDFSFile);
		
		for(FileStatus st: status) {
			Path temp = st.getPath();
			FSDataInputStream in = fs.open(temp);
			IOUtils.copyBytes(in, out,4096, false);
			in.close();
		}
		out.close();
	}
	public static void main(String[] args) throws IOException {
		String l = "/home/kqiao/hadoop/MyHadoop/putmergeFiles";
		String f = "hdfs://ubuntu:9000/user/kqiao/test/PutMergeTest";
		putMergeFunc(l,f);
	}
}
