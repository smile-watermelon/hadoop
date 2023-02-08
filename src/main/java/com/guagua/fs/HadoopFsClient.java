package com.guagua.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Optional;

/**
 * @author guagua
 * @date 2023/2/7 14:42
 * @describe
 */
public class HadoopFsClient {

    public HadoopFsClient() {
    }

    public static void main(String[] args) throws IOException {

    }

    /**
     * 创建目录
     *
     * @throws IOException
     */
    @Test
    public void mkdirTest() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-01:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.mkdirs(new Path("/user/guagua"));
        fileSystem.close();
        System.out.println("over");
    }

    /**
     * 删除目录/文件
     */
    @Test
    public void deleteDirTest() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-01:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        boolean delete = fileSystem.delete(new Path("/user/guagua"), false);
        System.out.println(delete);
    }

    /**
     * 上传文件
     */
    @Test
    public void copyFileToFsTest() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-01:9000"), conf, "guagua");
        fileSystem.copyFromLocalFile(new Path("data/guagua.txt"), new Path("/user/guagua/guagua.txt"));
        fileSystem.close();
    }

    /**
     * 重命名文件
     */
    @Test
    public void renameTest() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-01:9000"), conf, "guagua");
        fileSystem.rename(new Path("/user/guagua/guagua.txt"), new Path("/user/guagua/ke.txt"));
        fileSystem.close();
    }

    /**
     * 查看文件信息
     */
    @Test
    public void fileInfoTest() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-01:9000"), conf, "guagua");

        RemoteIterator<LocatedFileStatus> listFiles =
                fileSystem.listFiles(new Path("/user/guagua/ke.txt"), false);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            long blockSize = file.getBlockSize();
            short replication = file.getReplication();
            String owner = file.getOwner();
            String group = file.getGroup();
            Path path = file.getPath();
            long len = file.getLen();
            FsPermission permission = file.getPermission();
            String blockHosts = "";
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                blockHosts = Arrays.stream(hosts).reduce("", (v1, v2) -> v1 + " " + v2);
            }

            System.out.println(permission + " " + owner + " " + group + " " + len + " " + replication + " " + blockSize + blockHosts);
        }
    }

    /**
     * 判断是否是文件夹
     */
    @Test
    public void isDirTest() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-01:9000"), conf, "guagua");

        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            boolean file = fileStatus.isFile();
            if (file) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName() + File.separator);
            }
        }
    }

    /**
     * 上传IO流操作
     */
    @Test
    public void copyFileToFsIOTest() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-01:9000"), conf, "guagua");

        // 1、创建输入流
        FileInputStream fis = new FileInputStream(new File("data/guagua.txt"));

        // 2、创建输出流
        FSDataOutputStream fos = fileSystem.create(new Path("/user/guagua/guagua1.txt"));

        // 3、字节的拷贝
        IOUtils.copyBytes(fis, fos, conf);

        // 4、关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    /**
     * 只下载第一块
     * 下载IO流
     */
    @Test
    public void copyFileToLocalTest() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-01:9000"), conf, "guagua");

        // 1、创建输入流
        FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.7.tar.gz"));
        // 2、创建输出流
        FileOutputStream fos = new FileOutputStream("data/hadoop-2.7.7.tar.gz.part1");

        // 3、字节拷贝
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
//            int len;
//            while ((len = fis.read(buf)) != -1); {
//                fos.write(buf, 0, len);
//            }
            fos.write(buf);
        }
        // 4、关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }

    /**
     * 下载第二块
     */
    @Test
    public void readFileSeek2Test() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop-01:9000"), conf, "guagua");

        FSDataInputStream fis = fileSystem.open(new Path("/hadoop-2.7.7.tar.gz"));

        fis.seek(1024 * 1024 * 128);
        FileOutputStream fos = new FileOutputStream("data/hadoop-2.7.7.tar.gz.part2");

        // 方式一
//        byte[] buf = new byte[1024];
//        int len;
//        while ((len = fis.read(buf)) != -1) {
//            fos.write(buf, 0 ,len);
//        }

        IOUtils.copyBytes(fis, fos, conf);


        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fileSystem.close();
    }


}




















