package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-08
 */
public class TestFileCoherency {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://rhel-study01:9000/test/hello.txt");
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        fsDataOutputStream.write("helloworld 2019".getBytes());
        fsDataOutputStream.close();
        fs.close();

    }
}
