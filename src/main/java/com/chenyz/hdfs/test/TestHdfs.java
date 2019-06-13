package com.chenyz.hdfs.test;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-08
 */
public class TestHdfs {

    public static void main(String[] args) throws Exception {
        //注册协议处理器工程，让Java程序识别hdfs协议
        //URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());


        //定义url地址
        String url = "hdfs://rhel-study01:9000";
        //URL对象
        URL u= new URL(url);
        //URL连接
        URLConnection conn = u.openConnection();
        //打开输入流
        InputStream is = conn.getInputStream();
        //输出流
        FileOutputStream fos = new FileOutputStream("d:/hello.txt");
        byte[] buf = new byte[1024];
        int len = -1;
        while ((len =is.read(buf))!=-1){
            fos.write(buf,0,len);
        }
        is.close();
        fos.close();
        System.out.println("over");
                



    }
    



}
