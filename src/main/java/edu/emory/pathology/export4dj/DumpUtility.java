package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.CoPathCaseFinder;
import edu.emory.pathology.export4dj.finder.PathNetResultFinder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

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
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connCoPath = DriverManager.getConnection(priv.getProperty("connCoPath.url"));

        CoPathCaseFinder cpcf = new CoPathCaseFinder(connCoPath);
        PathNetResultFinder pnrf = new PathNetResultFinder(connCdw);

        Export4DJ export4DJ = new Export4DJ();
        export4DJ.coPathCases = new ArrayList<>();
        
        BufferedReader accNoReader = new BufferedReader(new FileReader(args[0]));
        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".csv", "") + ".export4dj.csv")));
        accNoWriter.println(CoPathCase.toStringHeader());
        String accNoReaderLine;
        while((accNoReaderLine = accNoReader.readLine()) != null) {
            String accNo = accNoReaderLine.split(",")[0];
            CoPathCase coPathCase = cpcf.getCoPathCaseByAccNo(accNo, pnrf);
            export4DJ.coPathCases.add(coPathCase);
            accNoWriter.println(coPathCase);
            accNoWriter.flush();
        }
        accNoReader.close();
        
        connCoPath.close();
        connCdw.close();

        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
        Marshaller m = jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, new Boolean(true));
        m.marshal(export4DJ, new FileOutputStream(new File(args[0].replace(".csv", "") + ".export4dj.xml")));
        
    }
    
}
