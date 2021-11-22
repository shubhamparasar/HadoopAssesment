package CreateDataAssignment1.UploadToHdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;

public class HdfsUpload {

    public static  final String HDFSNAME = "fs.defaultFS";
    public static final String HDFSPATH = "hdfs://localhost:8020/";

    public FileSystem fileUpload() throws Exception {
        System.out.println("Here We start Uplading To HDFS");
        String path = "/Users/meetgandhi/InpitCsv/";
        String uri = "hdfs://localhost:8020";
//        "hdfs://nameNomeHost:8020"
        String dir = "hdfs://localhost:8020/user/demodir/";

        File directoryPath = new File(path);
        String Contents[] = directoryPath.list();

        Configuration conf = new Configuration();
//        conf.set(HDFSNAME,HDFSPATH);

        FileSystem fs = FileSystem.get(new URI(HDFSPATH),conf);
//        System.out.println(Contents.length);
        for(String fileName : Contents) {
            fs.copyFromLocalFile(new Path(path+fileName+"/"), new Path(dir));
        }

        System.out.println("---------------->  All data is now loaded in Hdfs <-----------------------");
        return fs;
    }
}
