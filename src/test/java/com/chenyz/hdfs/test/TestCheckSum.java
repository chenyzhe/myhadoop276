package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-09
 */
public class TestCheckSum {

    @Test
    public void  testLocalFileSystem() throws Exception{
        //创建配置对象，加载core-default.xml + core-site.xml
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS","file:///");

        //
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("d:/hello.txt".toString());
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        fsDataOutputStream.write("helloworld".getBytes());
        fsDataOutputStream.close();
        fs.close();
        System.out.println("over");
    }


}
