package com.chenyz.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

/**
 * this is test case
 *
 * @author chenyz
 * @create 2019-06-08
 */
public class TestFileSystem {

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
        System.out.println("write over");

    }

    /*
    * 读取文件，从hdfs下载文件
    * */
    @Test
    public void readFile() throws Exception {

        Path path = new Path("hdfs://rhel-study01:9000/test/hello.txt");
        FSDataInputStream fis = fs.open(path);
        FileOutputStream fos = new FileOutputStream("d:/hello.txt");
        IOUtils.copyBytes(fis,fos,1024);
        System.out.println("readFile over!");
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }


    @Test
    public  void writeFileInReplication() throws Exception {

        Path path = new Path("hdfs://rhel-study01:9000/test/how.txt");
        FSDataOutputStream dos = fs.create(path,(short) 2);
        dos.write("how".getBytes());
        dos.close();
        System.out.println("write over !");

    }


    @Test
    public  void mkdir() throws Exception {

        Path path = new Path("/test/chenyz");
        //创建权限对象
        //FsPermission perm = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
        //创建目录，指定权限
        boolean b = fs.mkdirs(path, FsPermission.getDefault());
        System.out.println(b);

    }


    @Test
        public  void fileStatus() throws Exception {
        FileStatus filestatus = fs.getFileStatus(new Path("/test"));
        Class clazz = FileStatus.class;
        Method[] ms = clazz.getDeclaredMethods();
        for (Method m : ms) {
            String mname = m.getName();
            Class[] ptype = m.getParameterTypes();
            if (mname.startsWith("get") && (ptype == null || ptype.length == 0)) {
                if (!mname.equals("getSymlink")) {
                    Object ret = m.invoke(filestatus, null);
                    System.out.println(mname + "()=" + ret);
                }
            }
        }
    }


//    @Test
//    private void printlnPath(FileStatus fileStatus){
//        try {
//            Path path = fileStatus.getPath();
//            System.out.println(path.toUri().getPath());
//            if(fileStatus.isDirectory()){
//                //列出路径下的所有资源
//                FileStatus[] fileStatuses = fs.listStatus(path);
//                if(fileStatuses!=null &&fileStatuses.length>0){
//                    for (FileStatus ff:fileStatuses){
//                        printlnPath(ff);
//                    }
//                }
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public  void recursiveHDFSFile() throws Exception {
//        FileStatus fileStatus = fs.getFileStatus(new Path("/"));
//        printlnPath(fileStatus);
//    }

    @Test
    public void deleteFile() throws Exception {
        Path path = new Path("/test/chenyz");
        fs.delete(path,true);
    }


    @Test
    public void getBlockLocations() throws Exception{
        //创建路径
        Path path = new Path("/test/hello.txt");
        //获取文件状态
        FileStatus fileStatus = fs.getFileStatus(path);
        //获取文件大小
        long len = fileStatus.getLen();
        //得到文件块列表
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, len);
        //
        for(BlockLocation b:fileBlockLocations){
            System.out.println(b.getHosts());
        }
    }
}
