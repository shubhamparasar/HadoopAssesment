package ASSIGNMENT_4_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseConnect {

    public HbaseConnect(){
        System.out.println("in the hbaseconnect class");
    }

    public void createBuildingTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();

        configuration.addResource("/opt/homebrew/Cellar/hbase/2.4.6/libexec/conf/hbase-site.xml");
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.client.port","2181");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.master", "localhost:60000");

        System.out.println("configuration " +configuration);
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("connection "+ connection);
        Admin admin = connection.getAdmin();
        System.out.println("admin "+admin);

        if(admin.tableExists(TableName.valueOf("BUILDING"))){
            System.out.println("BUILDING Table Already Exists");
            return;
        }
        else {
            System.out.println("Table doesn't exist");
            TableDescriptorBuilder tableDescriptorbuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("BUILDING"));
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("buildingDetails")).build();
            tableDescriptorbuilder.setColumnFamily(columnFamilyDescriptor);
            admin.createTable(tableDescriptorbuilder.build());
            Table table = connection.getTable(TableName.valueOf("BUILDING"));

            System.out.println("BUILDING table created");

            table.close();
            connection.close();
            return;
        }

    }

    public void createEmployeeTable() throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        configuration.addResource("/opt/homebrew/Cellar/hbase/2.4.6/libexec/conf/hbase-site.xml");
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.client.port","2181");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.master", "localhost:60000");


        System.out.println("configuration " +configuration);
       Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("connection "+ connection);
        Admin admin = connection.getAdmin();
        System.out.println("admin "+admin);
        if(admin.tableExists(TableName.valueOf("EMPLOYEE"))){
            System.out.println("EMPLOYEE Table Already Exists");
            return;
        }
        else {
            System.out.println("Table doesn't exist");
            TableDescriptorBuilder tableDescriptorbuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("EMPLOYEE"));
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("employeeDetails")).build();
            tableDescriptorbuilder.setColumnFamily(columnFamilyDescriptor);
            admin.createTable(tableDescriptorbuilder.build());
            Table table = connection.getTable(TableName.valueOf("EMPLOYEE"));

            System.out.println("EMPLOYEE table created");

            table.close();
            connection.close();
            return;
        }






    }



    public void createCafeteriaEmployeeCodeTable() throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        configuration.addResource("/opt/homebrew/Cellar/hbase/2.4.6/libexec/conf/hbase-site.xml");
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.client.port","2181");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.master", "localhost:60000");


        System.out.println("configuration " +configuration);
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("connection "+ connection);
        Admin admin = connection.getAdmin();
        System.out.println("admin "+admin);
        if(admin.tableExists(TableName.valueOf("EMPLOYEE_CAFETERIA_CODE"))){
            System.out.println("EMPLOYEE Table Already Exists");
            return;
        }
        else {
            System.out.println("Table doesn't exist");
            TableDescriptorBuilder tableDescriptorbuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf("EMPLOYEE_CAFETERIA_CODE"));
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("employeeCafeteriaCodeDetails")).build();
            tableDescriptorbuilder.setColumnFamily(columnFamilyDescriptor);
            admin.createTable(tableDescriptorbuilder.build());
            Table table = connection.getTable(TableName.valueOf("EMPLOYEE_CAFETERIA_CODE"));

            System.out.println("EMPLOYEE_CAFETERIA_CODE table created");

            table.close();
            connection.close();
            return;
        }






    }
}
