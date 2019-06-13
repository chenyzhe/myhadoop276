package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.lang.reflect.Method;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-08
 */
public class TestFileCoherency {

    private Configuration conf;
    private FileSystem fs;
    @Before
    public void iniConf(){
        try {
            conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
            fs = FileSystem.get(conf);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /*
     * 写操作
     * */
    @Test
    public  void writeFile() throws Exception {

        Path path = new Path("hdfs://rhel-study01:9000/test/hello.txt");
        FSDataOutputStream dos = fs.create(path);
        dos.write("helloworld".getBytes());
        dos.close();

    }
}
