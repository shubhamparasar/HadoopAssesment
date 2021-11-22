package CreateDataAssignment1.CSVWriter;

import CreateDataAssignment1.PersonModel.Person;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.util.List;

public class WriteData {

    public void WriteCsv(List<Person> p, String path){
        try{
            CSVWriter writer = new CSVWriter(new FileWriter(path)) ;
            //Name, age, company, building_code, phone_number, address

            String header[] = {" Name","Age","Company","Building_Code","Phone_number","Address"};
            writer.writeNext(header);
            for(Person pers : p){
                writer.writeNext(new String[]{
                        pers.getName(),
                        String.valueOf(pers.getAge()), pers.getCompany(),
                        pers.getBuilding_code(),
                        pers.getPhone(),pers.getAddress() });
            }

            writer.close();
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }



}
