package com.chenyz.hdfs.codec;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-10
 */
public class TestCodec {


    /*
    * defalte
    * */
    @Test
    public void testDeflate() throws IOException {

        FileInputStream fis = new FileInputStream("d:/hello.txt");
        //创建编解码器对象
        DefaultCodec codec = new DefaultCodec();
        FileOutputStream fos = new FileOutputStream("d:/hello.deflate");
        //创建压缩流
        CompressionOutputStream outputStream = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis,outputStream,1024);
        fis.close();
        fos.close();
        outputStream.close();
    }


}
