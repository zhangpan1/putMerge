# putMerge
a demo  for HDFS
将文件放到HDFS中的时候，有一个问题使用put命令时
put启动耗时太长，如果文件数量太多，文件太小，会耗时很长
所以，我用了一个hdsf的api 批量读取文件 将读取完的文件
写入HDFS中
参数代表的含义如下：
args[0] 进来的目录 args[1] 出去的目录 args[2] hdfs的名字 args[3] 读取文件的大小 args[4]
