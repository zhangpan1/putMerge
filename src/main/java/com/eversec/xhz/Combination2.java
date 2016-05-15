package com.eversec.xhz;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Combination2 {
	public static void main(String[] args) {
		String path = "hdfs://mycluster/home/wyp/wyp.txt";//文件路径
		Configuration conf = new Configuration();
		conf.setBoolean("dfs.support.append", true);
		
		String inpath = "/home/wyp/append.txt";
		FileSystem fs = null;
		try{
			fs = FileSystem.get(URI.create(path), conf);
			//要追加的文件流，inpath为文件
			InputStream in = new 
					BufferedInputStream(new FileInputStream(inpath));
			OutputStream out = fs.append(new Path(path));
			IOUtils.copyBytes(in, out,4096,true);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
