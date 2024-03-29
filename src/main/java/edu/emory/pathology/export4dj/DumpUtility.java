package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.CoPathCaseFinder;
import edu.emory.pathology.export4dj.finder.DemographicsFinder;
import edu.emory.pathology.export4dj.finder.PathNetResultFinder;
import edu.emory.pathology.export4dj.finder.SebiaCaseFinder;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

/**
 *
 * @author Geoffrey H. Smith
 */
public class DumpUtility {

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, JAXBException {

        Properties priv = new Properties();
        try(InputStream inputStream = DumpUtility.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connCdw = DriverManager.getConnection(priv.getProperty("connCdw.url"), priv.getProperty("connCdw.u"), priv.getProperty("connCdw.p"));
        connCdw.setAutoCommit(false);
        connCdw.createStatement().execute("set role hnam_sel_all");

        PreparedStatement pstmtDx = connCdw.prepareStatement("select diagnosis_primary_hosp_cd, (select diagnosis_desc from ehcvw.lkp_diagnosis where diagnosis_key = redhp.diagnosis_key) x  from ehcvw.lkp_encounter le join ehcvw.rel_encounter_dx_hosp_primary redhp on(le.encounter_key = redhp.encounter_extension_key) where encounter_nbr = ?");
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connCoPath = DriverManager.getConnection(priv.getProperty("connCoPath.url"));

        CoPathCaseFinder cpcf = new CoPathCaseFinder(connCoPath);
        PathNetResultFinder pnrf = new PathNetResultFinder(connCdw);
        DemographicsFinder df = new DemographicsFinder(connCdw, connCoPath);

        //SebiaCaseFinder scf = new SebiaCaseFinder(new File(args[1]));
        SebiaCaseFinder scf = null;
        
        // new
        //Export4DJ export4DJ = new Export4DJ();
        //export4DJ.coPathCases = new ArrayList<>();

        //pickup where we left off
        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        Export4DJ export4DJ = (Export4DJ)unmarshaller.unmarshal(new FileInputStream("latest_xml_prepped/" + args[0] + ".export4dj.xml"));
        System.out.println(export4DJ.coPathCases.size() + " loaded");
        
        BufferedReader accNoReader = new BufferedReader(new FileReader(args[0]));
        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".csv", "") + ".export4dj.csv")));
        accNoWriter.println(CoPathCase.toStringHeader());
        String accNoReaderLine;
        while((accNoReaderLine = accNoReader.readLine()) != null) {
            try {
                String accNo = accNoReaderLine.split(",")[0].trim();
                System.out.print(accNo);
                
                CoPathCase coPathCase = null;

                for(CoPathCase candidateCoPathCase : export4DJ.coPathCases) {
                    if(candidateCoPathCase.accNo.equals(accNo)) {
                        coPathCase = candidateCoPathCase;
                        accNoWriter.println(coPathCase);
                        accNoWriter.flush();
                        System.out.println(": found");
                        break;
                    }
                }
                
                if(coPathCase == null) {

                    System.out.print(".");
                    coPathCase = cpcf.getCoPathCaseByAccNo(accNo, pnrf, scf);
                    System.out.print(".");
                    pstmtDx.setString(1, coPathCase.fin);
                    System.out.print(".");
                    ResultSet rsDx = pstmtDx.executeQuery();
                    System.out.print(".");
                    if(rsDx.next()) {
                        coPathCase.diagnosisCd = rsDx.getString(1);
                        coPathCase.diagnosisDesc = rsDx.getString(2);
                    }
                    if(coPathCase != null) {
                        export4DJ.coPathCases.add(coPathCase);
                        coPathCase.searchAccNo = accNo;
                        System.out.print(".");
                        coPathCase.demographics = df.getDemographicsByEmpiAndAccNo(coPathCase.empi, coPathCase.accNo);
                        System.out.print(".");
                        accNoWriter.println(coPathCase);
                        accNoWriter.flush();
                        System.out.println(": found");
                    }
                    else {
                        System.out.println(": NOT found");
                    }
                    
                }
                
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        accNoReader.close();
        accNoWriter.close();
        
        connCoPath.close();
        connCdw.close();

        jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
        Marshaller m = jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, new Boolean(true));
        m.marshal(export4DJ, new FileOutputStream(new File(args[0].replace(".csv", "") + ".export4dj.xml")));
        jc.generateSchema(new SchemaOutputResolver() {
            public Result createOutput(String namespaceURI, String suggestedFileName) throws IOException {
                File file = new File(args[0].replace(".csv", "") + ".export4dj.xsd");
                StreamResult result = new StreamResult(file);
                result.setSystemId(file.toURI().toURL().toString());
                return result;
            }                
        });
        
    }
    
}
