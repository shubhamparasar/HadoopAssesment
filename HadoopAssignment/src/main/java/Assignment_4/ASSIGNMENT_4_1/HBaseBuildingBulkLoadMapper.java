package ASSIGNMENT_4_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import proto.Building.BuildingOuterClass.*;
import proto.Employee.EmployeeOuterClass;

import java.io.IOException;

public class HBaseBuildingBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    public HBaseBuildingBulkLoadMapper() {
        System.out.println("in the BUILDING MAPPER class");
    }

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
    }

    ImmutableBytesWritable TABLE_NAME_TO_INSERT = new ImmutableBytesWritable(Bytes.toBytes("BUILDING"));
    int row = 1;

    public void map(LongWritable key, Text value, Context context) {
        try {

            String[] values = value.toString().split(",");
            System.out.println("values" + values);
     /*       EmployeeOuterClass.Employee employee = EmployeeOuterClass.Employee.newBuilder().setName(values[0])
                    .setID(Integer.parseInt(values[1]))
                    .setBuildingCode(Integer.parseInt(values[2]))
                    .setSalary(Integer.parseInt(values[3]))
                    .setDepartment(values[4])
                    .setFloorName(EmployeeOuterClass.Floor.forNumber(Integer.parseInt(values[5]))).build();*/


            Building building = Building.newBuilder()
                            .setBuildingCode(Integer.parseInt(values[0]))
                                    .setTotalFloor(Integer.parseInt(values[1]))
                                            .setNumberOfCompanies(Integer.parseInt(values[2]))
                                                    .setCafeteriaCode(Integer.parseInt(values[3])).build();


            System.out.println("building row " + row);
            System.out.println("building " + building);

            Put p = new Put(Bytes.toBytes(row++));


            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Building Code"), Bytes.toBytes(values[0]));
            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Total Floor"), Bytes.toBytes(values[1]));
            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Number of companies"), Bytes.toBytes(values[2]));
            p.addColumn(Bytes.toBytes("buildingDetails"), Bytes.toBytes("Cafeteria Code"), Bytes.toBytes(values[3]));
          //  p.addColumn(Bytes.toBytes("employeeDetails"), Bytes.toBytes("Department"), Bytes.toBytes(values[4]));
          //  p.addColumn(Bytes.toBytes("employeeDetails"), Bytes.toBytes("Floor Name"), Bytes.toBytes(values[5]));

            context.write(TABLE_NAME_TO_INSERT, p);

        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }
}
