package ASSIGNMENT_4_1;

import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import proto.Employee.EmployeeOuterClass.*;

import java.io.IOException;
import java.util.Arrays;

public class HBaseEmployeeBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {


    public HBaseEmployeeBulkLoadMapper() {
        System.out.println("in the EMPLOYEE MAPPER class");
    }

 /*   private String hbaseTable;
    private String dataSeperator;
    private String columnFamily1;
    private ImmutableBytesWritable hbaseTableName;

    /* public void setup(Context context){
         Configuration configuration = context.getConfiguration();
         hbaseTable = configuration.get("hbase.table.name");
         dataSeperator = configuration.get("data.separator");
         columnFamily1 = configuration.get("COLUMN_FAMILY_1");
         hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
     }*/
    int row = 1;
    ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes("EMPLOYEE"));
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


   /*     byte b[] = value.getBytes();
        EmployeeList employeeList = EmployeeList.parseFrom(Arrays.copyOf(value.getBytes(), value.getLength()));
        for (Employee employee : employeeList.getEmployeesList()) {
            int employee_id = employee.getID();
            System.out.println("employee row" + row);
            byte byteArray[] = employee.toByteArray();
            Put put = new Put(Bytes.toBytes(row++));
            put.addColumn(Bytes.toBytes("EMPLOYEE"), Bytes.toBytes("employeeDetails"), byteArray);
            context.write(immutableBytesWritable, put);*/

            System.out.println("in the Employee map function");

       String[] values = value.toString().split(",");
       System.out.println("values array "+values.length);
        Employee employee = Employee.newBuilder().setName(values[0])
                .setID(Integer.parseInt(values[1]))
                .setBuildingCode(Integer.parseInt(values[2]))
                .setSalary(Integer.parseInt(values[3]))
                .setDepartment(values[4])
                .setFloorName(Floor.forNumber(Integer.parseInt(values[5]))).build();

        System.out.println("data injected to proto object : " +employee);

        Put p = new Put(Bytes.toBytes(row++));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Name"),Bytes.toBytes(values[0]));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("ID"),Bytes.toBytes(values[1]));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Building Code"),Bytes.toBytes(values[2]));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Salary"),Bytes.toBytes(values[3]));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Department"),Bytes.toBytes(values[4]));
        p.addColumn(Bytes.toBytes("employeeDetails"),Bytes.toBytes("Floor Name"),Bytes.toBytes(values[5]));
        context.write(immutableBytesWritable,p);
        System.out.println("Employee mapper class end");
        }
    }
