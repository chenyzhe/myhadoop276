package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-11
 */
public class TestMapFile {

    @Test
    public void write() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/mapfile.map");
        //Writer writer = new Writer(conf,path);
        Writer writer = new Writer(conf,fs,path.toString(),IntWritable.class,Text.class);
        IntWritable key = new IntWritable();
        Text value = new Text();
        for(int i=1;i<100;i++){
            key.set(i);
            value.set("tom"+i);
            writer.append(key,value);

        }
        writer.close();

    }



    @Test
    public void writeWithInterval() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        //修改索引间隔
        conf.set("io.map.index.interval","20");

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/mapfile.map");
        Writer writer = new Writer(conf,fs,path.toString(),IntWritable.class,Text.class);
        writer.getIndexInterval();
        //修改索引间隔
        writer.setIndexInterval(10);

        IntWritable key = new IntWritable();
        Text value = new Text();
        for(int i=1;i<200;i=i+3){
            key.set(i);
            value.set("tom"+i);
            writer.append(key,value);

        }
        writer.close();

    }

    @Test
    public void read() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/mapfile.map");
        MapFile.Reader reader = new MapFile.Reader(fs,path.toString(),conf);
        IntWritable key = new IntWritable();
        Text value = new Text();

        while(reader.next(key,value)){
            System.out.println("key="+key+"  value="+value);
        }

        value = (Text) reader.get(new IntWritable(100),value);
        System.out.println(value);

        reader.close();
    }

    @Test
    public void seek() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/mapfile.map");
        MapFile.Reader reader = new MapFile.Reader(fs,path.toString(),conf);
        IntWritable key = new IntWritable();
        Text value = new Text();

        //value = (Text) reader.get(new IntWritable(8),value);
        key = (IntWritable) reader.getClosest(new IntWritable(197),value,true);
        System.out.println(key);

        reader.close();
    }

    @Test
    public void getClosest() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/mapfile.map");
        MapFile.Reader reader = new MapFile.Reader(fs,path.toString(),conf);
        IntWritable key = new IntWritable();
        Text value = new Text();

        reader.get(new IntWritable(9),value);
        System.out.println("key="+key+"  value="+value);

        reader.close();
    }



    @Test
    public void prepareSeqfile() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/seq/seq.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);

        IntWritable key =  new IntWritable(1);
        Text value = new Text();

        for(int i=1;i<=100;i++){
            key.set(i);
            value.set("tom"+i);
            writer.append(key,value);

        }
        writer.close();
        System.out.println("over");
    }

    /*
    * 需要把/test/seq/seq.seq 改为 /test/seq/data,map只能识别到data
    *
    * */
    @Test
    public void seq2map() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/seq");

        MapFile.fix(fs,path,IntWritable.class,Text.class,false,conf);
        System.out.println("over");


    }
}
