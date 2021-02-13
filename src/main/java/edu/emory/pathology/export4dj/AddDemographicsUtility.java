package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.DemographicsFinder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 *
 * @author Geoffrey H. Smith
 */
public class AddDemographicsUtility {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, JAXBException {

        Properties priv = new Properties();
        try(InputStream inputStream = DumpUtility.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connCdw = DriverManager.getConnection(priv.getProperty("connCdw.url"), priv.getProperty("connCdw.u"), priv.getProperty("connCdw.p"));
        connCdw.setAutoCommit(false);
        connCdw.createStatement().execute("set role hnam_sel_all");
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connCoPath = DriverManager.getConnection(priv.getProperty("connCoPath.url"));

        DemographicsFinder df = new DemographicsFinder(connCdw, connCoPath);
       
        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        Export4DJ export4DJ = (Export4DJ)unmarshaller.unmarshal(new FileInputStream(args[0]));

// temporary hack to get the subset columns in there            
Map<String, String> mrnReaderLines = new HashMap<>();
BufferedReader mrnReader = new BufferedReader(new FileReader(args[1]));
String mrnReaderLine;
while((mrnReaderLine = mrnReader.readLine()) != null) {
    if(mrnReaderLine.contains(", -->, \"")) {
        mrnReaderLines.put(mrnReaderLine.split(",")[8].replaceAll("\"", ""), mrnReaderLine.split(",")[0] + "," + mrnReaderLine.split(",")[1] + "," + mrnReaderLine.split(",")[2] + "," + mrnReaderLine.split(",")[3]);
    }
}
        
        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".xml", "") + ".with_demographics.csv")));
        accNoWriter.println("Pt No,record_id,dob,date of dx, , " + CoPathCase.toStringHeader());

        for(CoPathCase cpc : export4DJ.coPathCases) {
            System.out.println(cpc.accNo);
            cpc.demographics = df.getDemographicsByEmpiAndAccNo(cpc.empi, cpc.accNo);
            
// temporary hack to get the subset columns in there
            accNoWriter.println(mrnReaderLines.get(cpc.empi) + ", -->, " + cpc);
            accNoWriter.flush();
        }

        accNoWriter.close();
        
        Marshaller m = jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, new Boolean(true));
        m.marshal(export4DJ, new FileOutputStream(new File(args[0].replace(".xml", ".with_demographics.xml"))));
        
    }
    
}
