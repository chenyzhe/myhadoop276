package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-12
 */
public class TestArrayFile {

    @Test
    public void write() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        conf.set("io.map.index.interval","20");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/arrayfile");
        ArrayFile.Writer writer= new ArrayFile.Writer(conf,fs,path.toString(),Text.class);

        IntWritable key = new IntWritable();
        Text value = new Text();
        for(int i=1;i<100;i++){
            //key.set(i);
            value.set("tom"+i);
            //writer.append(key,value);
            writer.append(value);

        }
        writer.close();

    }


    @Test
    public void read() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        conf.set("io.map.index.interval","20");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/arrayfile");
        ArrayFile.Reader reader= new ArrayFile.Reader(fs,path.toString(),conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        while ((value=(Text)reader.next(value))!= null){
            System.out.println("  value="+value);
        }
        reader.close();

    }



}
