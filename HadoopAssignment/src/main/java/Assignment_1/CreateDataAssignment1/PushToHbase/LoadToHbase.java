package CreateDataAssignment1.PushToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class LoadToHbase {
    public  static FileSystem fs ;
    public static String uri = "hdfs://localhost:8020";
    public static String dir = "hdfs://localhost:8020/user/demodir";
    public static Path[] filePaths;


    public static void CreteTableHbase(){

    }

    public static void LoadConfg() throws Exception {

        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(uri) , conf);
        filePaths = readDirectoryContents();

    }

    private static Path[] readDirectoryContents() throws Exception {
        FileStatus[] fileStatus = fs.listStatus( new Path(dir));
        Path[] paths = FileUtil.stat2Paths(fileStatus);
        return paths;
    }

    public static void loadOnHbase()  {
        try{

            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(conf);

            // Verifying the existance of the table
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            Table table ;
            if( admin.tableExists(TableName.valueOf("people")) ){
                //Table Exist so just create connection
                System.out.println("here Admins");
                System.out.println("Table Already Exists No Need To Create");
                table = connection.getTable(TableName.valueOf("people"));
            }else{
                //Create Table
                // Instantiating table descriptor class
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));

                // Adding column families to table descriptor
                //Name, age, company, building_code, phone_number, address
                tableDescriptor.addFamily(new HColumnDescriptor("peoples"));

                tableDescriptor.addFamily(new HColumnDescriptor("name"));
                tableDescriptor.addFamily(new HColumnDescriptor("age"));
                tableDescriptor.addFamily(new HColumnDescriptor("company"));
                tableDescriptor.addFamily(new HColumnDescriptor("building_code"));
                tableDescriptor.addFamily(new HColumnDescriptor("phone_number"));
                tableDescriptor.addFamily(new HColumnDescriptor("address"));



                // Execute the table via admin
                System.out.println("creating table... ");

                admin.createTable(tableDescriptor);
                System.out.println("Done! ");
                table = connection.getTable(TableName.valueOf("people"));}
            int rowNum = 0;
            for(Path path : filePaths){
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                line=br.readLine();
                if(path.toString().contains(".csv") == false) {
                    continue;
                }
                System.out.println(path.toString());
                while(line !=null){
                    if(rowNum == 0){
                        rowNum++;
                        line= br.readLine();
                        continue;
                    }else{
                    String[] fields = line.split(",");
                    Put p = new Put(Bytes.toBytes("row" + rowNum));
                        System.out.println(line );
                        p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("name"), Bytes.toBytes(fields[0]));
                        p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("age"), Bytes.toBytes(fields[1]));
                        p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("company"), Bytes.toBytes(fields[2]));
                        p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("building_code"), Bytes.toBytes(fields[3]));
                        p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("phone_number"), Bytes.toBytes(fields[4]));
                        p.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("address"), Bytes.toBytes(fields[5]));
                        table.put(p);
                    System.out.println("Inserted Row" + rowNum);
                    rowNum++;
                    line=br.readLine();
                    }
                }
                rowNum= 0;

            }
            table.close();
            connection.close();
            fs.close();



        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public static void LoadIntoHbases() throws Exception {

//        filePaths = readDirectoryContents();
        LoadConfg();
        loadOnHbase();
    }
    public static void main(String[] args) throws Exception {
//        LoadConfg();
//        loadOnHbase();
        LoadIntoHbases();
    }
}
