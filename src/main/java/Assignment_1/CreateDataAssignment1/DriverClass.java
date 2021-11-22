package CreateDataAssignment1;

import CreateDataAssignment1.CSVWriter.WriteData;
import CreateDataAssignment1.PersonModel.Person;
import CreateDataAssignment1.PushToHbase.LoadToHbase;
import CreateDataAssignment1.UploadToHdfs.HdfsUpload;
import com.github.javafaker.Faker;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DriverClass {

    private static Person getPerson(){
        Faker fke = new Faker();
        String Name = fke.name().fullName();
        Integer age = fke.number().numberBetween(19,69);
        String company = fke.company().toString();
        String Building_Code = fke.code().toString();
        String phone = fke.phoneNumber().cellPhone();
        String address = fke.address().fullAddress();

        Person per = new Person();
        per.setAddress(address);
        per.setAge(age);
        per.setBuilding_code(Building_Code);
        per.setCompany(company);
        per.setPhone(phone);
        per.setName(Name);
        return per;

    }
    public static int counts = 0;
    private static List<Person> addPersontoAline(int count){
        counts = count;
        List<Person> arrPerson = new ArrayList<>();
        if(count%2 == 0)
            for(int i = 0 ;i < 9;i++){ arrPerson.add(getPerson());}
        else
            for(int i = 0 ;i < 15;i++){ arrPerson.add(getPerson()); }

        return arrPerson;
    }

    private  static void WriteToCsv(){
        String Path ="/Users/meetgandhi/InpitCsv/";
        File directoryPath = new File(Path);
        String Contents[] = directoryPath.list();
        if(Contents.length >= 99){
            return ;
        }
        for(int i = 0 ; i < 100 ; i++){
            List<Person> person = addPersontoAline(i);
            //Now Create CSV and add the data here
            WriteData newData = new WriteData();
            String filepath = "/Users/meetgandhi/InpitCsv/File"+String.valueOf(counts++)+".csv";
            newData.WriteCsv(person,filepath);
        }
        System.out.println("------> Data is Now Been Created in Local Dir <--------");
    }

    public static void main(String[] args) throws Exception {
        //Here We Create Data and Store Locally 100 csv files
        WriteToCsv();

        //Now we Upload this to HDFS
        HdfsUpload hdu = new HdfsUpload();
        FileSystem fs = hdu.fileUpload();

        Thread.sleep(1000);

        //Here we Load All this files to Hbase Table
        LoadToHbase hb = new LoadToHbase();
//        hb.LoadIntoHbases(fs);

    }


}
