package Hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

public class HdfsWriter {

    private static final Logger logger = Logger.getLogger("io.saagie.example.hdfs.Main");
    private final String HDFSURI = "hdfs://quickstart.cloudera:8020";
    private final Configuration CONF = new Configuration();
    private final FileSystem FS;

    public HdfsWriter() throws IOException {
        // Set FileSystem URI
        CONF.set("fs.defaultFS", HDFSURI);
        // Because of Maven
        CONF.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        CONF.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/var/lib/hadoop-hdfs/");
        //Get the filesystem - HDFS
        this.FS = FileSystem.get(URI.create(HDFSURI), CONF);
    }

    public void closeFileSystem() throws IOException {
        this.FS.close();
    }

    public void writeLineIntoOutputStream(String fileContent, FSDataOutputStream outputStream) throws IOException {

        //==== Write line
        logger.info("Begin Write line into file");

        //Cassical output stream usage
        outputStream.writeBytes(fileContent);
        logger.info("End Write line into hdfs");

    }

    private Path createFolder(String path) throws IOException {
        Path workingDir = FS.getWorkingDirectory();
        System.out.println(workingDir);
        Path newFolderPath = new Path(path);
        if (!FS.exists(newFolderPath)) {
            // Create new Directory
            FS.mkdirs(newFolderPath);
            logger.info("Path " + path + " created.");
        }
        return newFolderPath;
    }

    public FSDataOutputStream createFileAndOutputStream(String path, String fileName) throws IOException {
        Path newFolderPath = this.createFolder(path);
        //Create a path
        //Init output stream
        return FS.create(new Path(newFolderPath + "/" + fileName));
    }


}