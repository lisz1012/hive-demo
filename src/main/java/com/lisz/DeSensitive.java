package com.lisz;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/*
使用方法：大好jar包，然后传到hive的某一台机器上面，然后在它上面敲hive进入hive命令行执行：
add jar /root/hive-demo-1.0-SNAPSHOT.jar;
然后再在hive命令行下创建自定义的函数：
create temporary function de_sensitive as 'com.lisz.DeSensitive';
再使用这个函数：
hive> select de_sensitive(name) from psn;
OK
_c0
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz
Time taken: 0.402 seconds, Fetched: 9 row(s)
注意，这种方法每次重新登录hive命令行都会失效。
可以把jar包上传到hdfs：
hdfs dfs -mkdir /jar
hdfs dfs -put ./*.jar /jar
再：
hive> create function de_sensitive2 as 'com.lisz.DeSensitive' using jar 'hdfs://mycluster/jar/hive-demo-1.0-SNAPSHOT.jar';

然后再执行：
hive> select de_sensitive2(name) from psn;
OK
_c0
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz
小lisz

退出之后也可以再直接执行：select de_sensitive2(name) from psn;但第一次会看到输出，先要add相关的jar
所以推荐传到hdfs
MySQL的元数据存在了 FUNC中
参考：https://cwiki.apache.org/confluence/display/Hive/HivePlugins
 */
public class DeSensitive extends UDF {
	// 方法名必须叫 evaluate
	public Text evaluate(final Text t) {
		if (t == null) return null;
		String str = t.toString().substring(0, 1) + "lisz";
		return new Text(str);
	}
}
