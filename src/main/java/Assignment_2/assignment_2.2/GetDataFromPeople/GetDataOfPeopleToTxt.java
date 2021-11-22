package Assignment2.GetDataFromPeople;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class GetDataOfPeopleToTxt {

    //first we extract data from table to txt and than upload that txt data use bulkLoad.
      public final static String outputFilePath = "/Users/meetgandhi/Documents/MeetDocs/outputinTxt.txt";

     public static void getDataFomHbase(Configuration conf, HBaseAdmin admin,
                                        Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("people"));
         Scan scan = new Scan();
         ResultScanner resultScanner = null;

         scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("name"));
         scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("age"));
         scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("company"));
         scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("building_code"));
         scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("phone_number"));
         scan.addColumn(Bytes.toBytes("peoples"), Bytes.toBytes("address"));

         resultScanner = table.getScanner(scan);
         BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath));


         for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
             byte[] name = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("name"));
             byte[] agr = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("age"));
             byte[] company = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("company"));
             byte[] building_code = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("building_code"));
             byte[] phone = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("phone_number"));
             byte[] address = result.getValue(Bytes.toBytes("peoples"), Bytes.toBytes("address"));

             String Fname = Bytes.toString(name);
             String age = Bytes.toString(agr);
             String Company = Bytes.toString(company);
             String code = Bytes.toString(building_code);
             String no = Bytes.toString(phone);
             String Adddrss = Bytes.toString(address);

             String lines = Fname+","+age+","+Company+","+code+","+no+","+Adddrss;
             writer.write(lines);



             System.out.println(Fname +" " +age +" "+Company+" "+code+" "+no+" "+Adddrss);
         }
         writer.close();

         }

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Thread.sleep(1000);
        // Verifying the existance of the table
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        Table table  ;
        Thread.sleep(1000);
        if( admin.tableExists(TableName.valueOf("people")) ){
            //Table Exist so just create connection
            getDataFomHbase(conf,admin,connection);
        }else{
            System.out.println("No Table of people data exists.");
        }
        connection.close();
}
}

