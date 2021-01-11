package com.test;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsFileUtils {

    public static FileSystem fileSystem = null;

    static {
        try {
            fileSystem = getFileSystem();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 连接高可用HA
    public static FileSystem getFileSystem() throws Exception {
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata02:9000"),
//                new Configuration(), "bigdata");

        System.setProperty("HADOOP_USER_NAME", "bigdata");
        Configuration conf = new Configuration();
        String confDir = HdfsFileUtils.class.getClassLoader().getResource("conf").toString();
        System.out.println(confDir);
        conf.addResource(new Path(FilenameUtils.concat(confDir, "core-site.xml")));
        conf.addResource(new Path(FilenameUtils.concat(confDir, "hdfs-site.xml")));
        FileSystem fileSystem = FileSystem.get(conf);
        return fileSystem;
    }


    /**
     * 小文件的合并上传
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void uploadDirToFile(String localDir, String remoteFile) throws URISyntaxException, IOException,
            InterruptedException {
        FSDataOutputStream outputStream = fileSystem.create(new
                Path(remoteFile));
        LocalFileSystem localFileSystem = FileSystem.getLocal(new
                Configuration());
        FileStatus[] fileStatuses = localFileSystem.listStatus(new
                Path(localDir));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream =
                    localFileSystem.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }

    /**
     * 小文件的合并下载
     * @param remoteDir
     * @param localFile
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void downloadDirToFile(String remoteDir, String localFile) throws URISyntaxException, IOException,
            InterruptedException {
        BufferedOutputStream outputStream = new
                BufferedOutputStream(new FileOutputStream(localFile));
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new
                Path(remoteDir), true);
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("开始合并下载：" + fileStatus.getPath());
            FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        System.out.println("合并下载完成");
        outputStream.flush();
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }


    public static void main(String[] args) {
        HdfsFileUtils hdfsFileUtils = new HdfsFileUtils();
        try {
            hdfsFileUtils.uploadDirToFile("D:\\bigdata\\input", "/test_big.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            hdfsFileUtils.downloadDirToFile("/test", "D:\\bigdata\\test.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

