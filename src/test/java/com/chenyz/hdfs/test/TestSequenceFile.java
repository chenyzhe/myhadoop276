package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Test;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-10
 */
public class TestSequenceFile {

    @Test
    public void write() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/myseqfile.seq");
        Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);

        IntWritable key =  new IntWritable(1);
        Text value = new Text();

        for(int i=0;i<100;i++){
            key.set(i);
            value.set("tom"+i);
            writer.append(key,value);

        }
        writer.close();
        System.out.println("over");
    }

    @Test
    public void readSeq() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/myseqfile.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
        IntWritable key = new IntWritable();
        Text value = new Text();

        while(reader.next(key,value)){
            System.out.println("key="+key+"  value="+value);
        }

    }


    @Test
    public void getSeqFile() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/myseqfile.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
        IntWritable key = new IntWritable();
        Text value = new Text();

        //key类型
        Class keyClass = reader.getKeyClass();
        //value类型
        Class valClass = reader.getValueClass();
        //得到压缩类型
        SequenceFile.CompressionType type = reader.getCompressionType();
        //得到压缩编解码器
        CompressionCodec codec = reader.getCompressionCodec();
        //得到当前key对应的字节数
        long position = reader.getPosition();
        //System.out.println(position);

        while(reader.next(key,value)){
            System.out.println("Posititon="+position+"  key="+key+"  value="+value);
            position = reader.getPosition();
        }

    }




    @Test
    public void seekSeq() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/myseqfile.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        //定位指针到指定位置
        reader.seek(253);
        reader.next(key,value);
        System.out.println("key="+key+"  value="+value);
        reader.close();
    }


    @Test
    public void readWithSync() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/myseqfile.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
        IntWritable key = new IntWritable();
        Text value = new Text();

        int syncPos = 595;
        //定位到下一个同步点
        reader.sync(syncPos);
        //得到当前指针位置
        long position = reader.getPosition();
        reader.next(key,value);
        System.out.println(syncPos+":"+position+":"+key+":"+value);

    }


    @Test
    public void writeWithSync() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/myseqfile.seq");
        Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);

        IntWritable key =  new IntWritable(1);
        Text value = new Text();

        for(int i=0;i<100;i++){
            key.set(i);
            value.set("tom"+i);
            writer.append(key,value);
            if(i%5 == 0){
                //创建同步点
                writer.sync();
            }
        }
        writer.close();
        System.out.println("over");
    }

    @Test
    public void writeInCompress() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://rhel-study01:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/test/myseqfile2.seq");
        Writer writer = SequenceFile.createWriter(fs,conf, path, IntWritable.class, Text.class, SequenceFile.CompressionType.NONE);
        IntWritable key =  new IntWritable(1);
        Text value = new Text();

        for(int i=0;i<100;i++){
            key.set(i);
            value.set("tom"+i);
            writer.append(key,value);

        }
        writer.close();
        System.out.println("over");
    }

}
