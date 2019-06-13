package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-12
 */
public class TestSetFile {

    @Test
    public void write() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        conf.set("io.map.index.interval","20");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/setfile");
        SetFile.Writer writer= new SetFile.Writer(fs,path.toString(), IntWritable.class);

        IntWritable key = new IntWritable();
        Text value = new Text();
        for(int i=1;i<100;i++){
            key.set(i);
            writer.append(key);

        }
        writer.close();

    }


    @Test
    public void read() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        conf.set("io.map.index.interval","20");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/setfile");
        SetFile.Reader reader= new SetFile.Reader(fs,path.toString(),conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key)){
            System.out.println("  key="+key);
        }
        reader.close();

    }
}
