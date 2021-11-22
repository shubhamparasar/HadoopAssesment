package ASSIGNMENT_4_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import proto.Employee.EmployeeOuterClass.*;

import java.util.Arrays;

public class TempEmployeeMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    public TempEmployeeMapper(){
        System.out.println("in the mapper class");
    }


    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
    }

    ImmutableBytesWritable TABLE_NAME_TO_INSERT = new ImmutableBytesWritable(Bytes.toBytes("EMPLOYEE"));
    int row = 1;
    public void map(LongWritable key, Text value, Context context) {
        try {

            String[] values = value.toString().split(",");
            System.out.println("values" +values);
            Employee employee = Employee.newBuilder().setName(values[0])
                    .setID(Integer.parseInt(values[1]))
                    .setBuildingCode(Integer.parseInt(values[2]))
                    .setSalary(Integer.parseInt(values[3]))
                    .setDepartment(values[4])
                    .setFloorName(Floor.forNumber(Integer.parseInt(values[5]))).build();


                System.out.println("employee row " + row);
                System.out.println("employee "+employee);

                Put p = new Put(Bytes.toBytes(row++));


            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Name"),Bytes.toBytes(values[0]));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("ID"),Bytes.toBytes(values[1]));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Building Code"),Bytes.toBytes(values[2]));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Salary"),Bytes.toBytes(values[3]));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Department"),Bytes.toBytes(values[4]));
            p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Floor Name"),Bytes.toBytes(values[5]));

            context.write(TABLE_NAME_TO_INSERT, p);

        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}
