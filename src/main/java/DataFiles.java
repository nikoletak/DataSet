import java.io.*;
import java.sql.*;
import java.util.Arrays;

public class DataFiles {
    public static void main(String[] args)throws Exception{

        String url = "jdbc:mysql://127.0.0.1:3306/paurus_db"; //DB URL
        String username = "root"; // DB credentials
        String password = "Charlie19952@";
        Connection con = DriverManager.getConnection(url, username, password); //connecting with DB with provided credentials
        con.setAutoCommit(false);

        BufferedReader reader;
        reader = new BufferedReader(new FileReader("fo_random.txt"));//read from file
        String lineData = reader.readLine();//read line by line
        int counter = 0; //for checking parsing elements
        //inserting into DB
        PreparedStatement stmt = con.prepareStatement("INSERT INTO Paurus(MATCH_ID, MARKET_ID, OUTCOME_ID,SPECIFIERS) VALUES (?, ?, ?, ?)");

        while (lineData != null) {
            if(counter > 0) {
                System.out.println(lineData);
                String[] arrLineData = lineData.split("\\|");

                String arrLastElem[] = {} ;
                if (arrLineData.length > 3){
                    //everything after 3. element goes into SPECIFIERS Column
                    arrLastElem = Arrays.copyOfRange(arrLineData, 3, arrLineData.length);
                }
                //setting our values into columns
                stmt.setInt(1, Integer.parseInt(arrLineData[0].split(":")[2].replace("'","")));
                stmt.setString(2, arrLineData[1]);
                stmt.setString(3, (arrLineData[2].replace("'","")));
                stmt.setString(4, String.join("\\|", arrLastElem).replace("'",""));


                stmt.addBatch();

            }
            lineData = reader.readLine();
            counter++;
        }
        stmt.executeBatch();
        con.commit();
        reader.close();
        stmt.close(); // close statement
        con.close(); // close connection
    }

}
