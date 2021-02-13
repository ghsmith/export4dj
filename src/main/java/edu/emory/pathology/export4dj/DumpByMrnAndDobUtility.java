package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.CoPathCaseFinder;
import edu.emory.pathology.export4dj.finder.DemographicsFinder;
import edu.emory.pathology.export4dj.finder.PathNetResultFinder;
import edu.emory.pathology.export4dj.finder.SebiaCaseFinder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

/**
 *
 * @author Geoffrey H. Smith
 */
public class DumpByMrnAndDobUtility {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, JAXBException, ParseException {

        Properties priv = new Properties();
        try(InputStream inputStream = DumpByMrnAndDobUtility.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connCdw = DriverManager.getConnection(priv.getProperty("connCdw.url"), priv.getProperty("connCdw.u"), priv.getProperty("connCdw.p"));
        connCdw.setAutoCommit(false);
        connCdw.createStatement().execute("set role hnam_sel_all");
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connCoPath = DriverManager.getConnection(priv.getProperty("connCoPath.url"));

        CoPathCaseFinder cpcf = new CoPathCaseFinder(connCoPath);
        PathNetResultFinder pnrf = new PathNetResultFinder(connCdw);
        DemographicsFinder df = new DemographicsFinder(connCdw, connCoPath);

        //SebiaCaseFinder scf = new SebiaCaseFinder(new File(args[1]));
        SebiaCaseFinder scf = null;
        
        Export4DJ export4DJ = new Export4DJ();
        export4DJ.coPathCases = new ArrayList<>();
        
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
        BufferedReader mrnReader = new BufferedReader(new FileReader(args[0]));
        //String mrnHeaders = mrnReader.readLine();
        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".csv", "") + ".export4dj.csv")));
        accNoWriter.println(CoPathCase.toStringHeader());
        try {
            String mrnReaderLine;
            while((mrnReaderLine = mrnReader.readLine()) != null) {
                String mrn = mrnReaderLine.split(",")[1];
                Date dob = null;
                if(mrnReaderLine.split(",").length >= 3 && mrnReaderLine.split(",")[2] != null && mrnReaderLine.split(",")[2].length() > 0) {
                    dob = new Date(sdf.parse(mrnReaderLine.split(",")[2]).getTime());
                }
                List<CoPathCase> coPathCases = null;
                if(dob != null) {
                    coPathCases = cpcf.getCoPathCasesByFacilityMrnAndDob(mrn, dob, pnrf, scf);
                }
                else {
                    coPathCases = cpcf.getCoPathCasesByFacilityMrn(mrn, pnrf, scf);
                }
                export4DJ.coPathCases.addAll(coPathCases);
                for(CoPathCase coPathCase : coPathCases) {
                    coPathCase.searchPtNo = mrnReaderLine.split(",")[0];
                    coPathCase.searchRecordId = mrnReaderLine.split(",")[1];
                    coPathCase.searchDob = dob;
                    if(mrnReaderLine.split(",").length >= 4 && mrnReaderLine.split(",")[3] != null && mrnReaderLine.split(",")[3].length() > 0) {
                        coPathCase.searchDateOfDx = new Date(sdf.parse(mrnReaderLine.split(",")[3]).getTime());
                    }
                    coPathCase.demographics = df.getDemographicsByEmpiAndAccNo(coPathCase.empi, coPathCase.accNo);
                    accNoWriter.println(coPathCase);
                }
                accNoWriter.flush();
            }
            mrnReader.close();
            accNoWriter.close();

            connCoPath.close();
            connCdw.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
            
        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
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
