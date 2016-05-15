package com.eversec.xhz;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge {
	//输入流
	private InputStream in = null;
	//输出流
	private OutputStream out = null;
	//要读取的文件夹(名)
	private String localPath;
	//hdfs上的文件名
	private String hdfsPath;
	//文件写完以后,要修改的名字
	private String changeName;
	//要连接的hdfs
	private String namenode;
	//入库的类型
	//private String type;
	//每次都去的字节数
	private String size;
	//put以后要更改的后缀
	private String rename;
	//入库的日期小时
	private String date;
	//初始化参数
	public PutMerge(String localPath, String hdfsPath, String namenode,/*String type,*/
			String size, String rename) {
		this.localPath = localPath;
		this.hdfsPath = hdfsPath;
		this.changeName=this.hdfsPath.replace(".coppy", "");
		//this.date = new Date(); this.sd = new SimpleDateFormat("yyyyMMddHH");
		this.namenode = namenode;
		//this.type=type;
		this.size = size;
		this.rename = rename;
		this.date = hdfsPath.substring(5, 16).replace("/", "");
	}

	public void start() throws Exception {
		File file = new File(localPath);
		Configuration conf = new Configuration();
		conf.set("fs.default.name", namenode);//操作hdfs
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		out = fs.create(new Path(hdfsPath));//获取hdfs的输出流

		work(file, size);

		close();
		rename(hdfsPath,changeName,fs);
	}

	/**
	 * 1.根据用户定义的参数设置本地目录和HDFS的目标文件 2.创建一个输出流写入到HDFS文件
	 * 3.遍历本地目录中的每个文件，打开文件，并读取文件内容，将文件的内容写到HDFS文件中
	 * 
	 * @throws IOException
	 * 
	 */
	private void work(File file, String size) throws IOException {
		int s = Integer.parseInt(size);
		if (file.isFile() && file.getAbsolutePath().contains(date)
				          && file.getAbsolutePath().endsWith(".txt.gz")
				/*&& file.getAbsolutePath().contains(type)*/) {
			int byteRead = 0;
			in = new GZIPInputStream(new FileInputStream(
					file));
			byte[] buffer = new byte[s];
			while ((byteRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, byteRead);
			}
			file.renameTo(new File(file.getName() + rename));
		} else if (file.isDirectory()) {
			File[] files = file.listFiles();
			try {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isFile()
							&& files[i].getAbsolutePath().contains(date)
							&& files[i].getAbsolutePath().endsWith(".txt.gz")
							
							/*&& files[i].getAbsolutePath().contains(type)*/) {
						System.out.println(files[i]);
						in = new GZIPInputStream(new FileInputStream(
								files[i]));
						int byteRead = 0;
						byte[] buffer = new byte[s];
						while ((byteRead = in.read(buffer)) > 0) {
							out.write(buffer, 0, byteRead);
						}
						in.close();

					} else {
						work(files[i], size);
					}
				}
				for (int i = 0; i < files.length; i++) {
					if (files[i].isFile()
							&& files[i].getAbsolutePath().contains(date)
							&& files[i].getAbsolutePath().endsWith(".txt.gz")
							/*&& files[i].getAbsolutePath().contains(type)*/) {
						System.out.println(files[i].renameTo(new File(files[i]
								.getAbsolutePath() + rename)));
					}
				}
				
			} catch (Exception e) {
				System.out.println(e);
				e.printStackTrace();
				for (int i = 0; i < files.length; i++) {
					if (files[i].isFile()
							&& files[i].getAbsolutePath().contains(date)) {
						System.out.println(files[i].renameTo(new File(files[i]
								.getAbsolutePath() + ".ERROR")));
					}
				}

			}
		}
	}

	private void close() throws IOException {
		if (in != null) {
			in.close();
		}
		if (out != null) {
			out.close();
		}
	}
	//写完文件以后改名
	private void rename(String str1,String str2,FileSystem fs){
		Path fsPath = new Path(str1);
		Path toPath = new Path(str2);
		try {
			boolean isRename = fs.rename(fsPath, toPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 主方法
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage:\n\t" + PutMerge.class.getName()
					+ "[LocalPath] [HDFSPath]");
			System.exit(1);
			
		}
	
		/**
		 * args[0] 进来的目录 args[1] 出去的目录 args[2] hdfs的名字 args[3] 读取文件的大小 args[4]
		 * 入库成功以后更改文件名
		 */
		new PutMerge(args[0], args[1], args[2], args[3], args[4]).start();
	}
}
