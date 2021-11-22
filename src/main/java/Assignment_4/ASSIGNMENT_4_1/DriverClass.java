package ASSIGNMENT_4_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URI;

public class DriverClass {
    public static void main(String[] args) throws Exception {


     Configuration conf = new Configuration();
     System.out.println("configuration = " +conf);
     FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000/"), conf);
     System.out.println("File System = " + hdfs);
     hdfs.delete(new Path("hdfs://localhost:9000/user"),true);
     //   hdfs.delete(new Path("hdfs://localhost:9000/Employee.csv"),true);
        hdfs.delete(new Path("hdfs://localhost:9000/BuildingResult"),true);
    // hdfs.copyFromLocalFile(new Path("/Users/shubhamparasar/HadoopConnect/Employee.csv"),new Path("hdfs://localhost:9000/"));
     //   hdfs.copyFromLocalFile(new Path("/Users/shubhamparasar/HadoopConnect/Building.csv"),new Path("hdfs://localhost:9000/"));



    }
}
