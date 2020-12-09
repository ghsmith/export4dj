package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.CoPathCaseFinder;
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

        SebiaCaseFinder scf = new SebiaCaseFinder(new File(args[1]));
        
        Export4DJ export4DJ = new Export4DJ();
        export4DJ.coPathCases = new ArrayList<>();
        
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
        BufferedReader mrnReader = new BufferedReader(new FileReader(args[0]));
        String mrnHeaders = mrnReader.readLine();
        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".csv", "") + ".export4dj.csv")));
        accNoWriter.println(mrnHeaders + ", , " + CoPathCase.toStringHeader());
        String mrnReaderLine;
        while((mrnReaderLine = mrnReader.readLine()) != null) {
            String mrn = mrnReaderLine.split(",")[1];
            Date dob = new Date(sdf.parse(mrnReaderLine.split(",")[2]).getTime());
            List<CoPathCase> coPathCases = cpcf.getCoPathCasesByFacilityMrnAndDob(mrn, dob, pnrf, scf);
            export4DJ.coPathCases.addAll(coPathCases);
            for(CoPathCase coPathCase : coPathCases) {
                accNoWriter.println(mrnReaderLine + ", -->, " + coPathCase);
            }
            accNoWriter.flush();
        }
        mrnReader.close();
        accNoWriter.close();
        
        connCoPath.close();
        connCdw.close();

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
