package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Demographics;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.DemographicsFinder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 *
 * @author Geoffrey H. Smith
 */
public class AddDemographicsUtility {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, JAXBException, ParseException {

        Map<String, Demographics> demoMap = new HashMap<>();
        {
            Pattern pattern = Pattern.compile("^\"([^\"]*)\",\"[^\"]*\",\"[^\"]*\",\"[^\"]*\",\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\",.*$");
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            br.readLine();
            String line;
            while((line = br.readLine()) != null) {
                if(line.startsWith("\"")) {
                    Matcher matcher = pattern.matcher(line);
                    if(matcher.matches() && matcher.group(2).length() > 0) {
                        Demographics demographics = new Demographics();
                        demographics.birthDate = matcher.group(2).length() > 0 ? new java.sql.Date(sdf.parse(matcher.group(2)).getTime()) : null;
                        demographics.deathDate = matcher.group(3).length() > 0 ? new java.sql.Date(sdf.parse(matcher.group(3)).getTime()) : null;
                        demographics.ethnicity = matcher.group(4).length() > 0 ? matcher.group(4) : null;
                        demographics.race = matcher.group(5).length() > 0 ? matcher.group(5) : null;
                        demographics.ethnicGroup = matcher.group(6).length() > 0 ? matcher.group(6) : null;
                        demographics.gender = matcher.group(7).length() > 0 ? matcher.group(7) : null;
                        demographics.zipCode = matcher.group(8).length() > 0 ? matcher.group(8) : null;
                        demoMap.put(matcher.group(1), demographics);
                    }
                }
            }
        }
        
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

        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".xml", "") + ".with_demographics.csv")));
        accNoWriter.println(CoPathCase.toStringHeader());

        for(CoPathCase cpc : export4DJ.coPathCases) {
            System.out.println(cpc.accNo);
            if(demoMap.get(cpc.accNo) != null) {
                cpc.demographics = demoMap.get(cpc.accNo);
            }
            else {
                cpc.demographics = df.getDemographicsByEmpiAndAccNo(cpc.empi, cpc.accNo);
            }
            accNoWriter.println(cpc);
            accNoWriter.flush();
        }

        accNoWriter.close();
        
        Marshaller m = jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, new Boolean(true));
        m.marshal(export4DJ, new FileOutputStream(new File(args[0].replace(".xml", ".with_demographics.xml"))));
        
    }
    
}
