package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.finder.CoPathCaseFinder;
import edu.emory.pathology.export4dj.finder.PathNetResultFinder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author Geoffrey H. Smith
 */
public class DumpUtility {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

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

        System.out.println(CoPathCase.toStringHeader());
        
        BufferedReader accNoReader = new BufferedReader(new FileReader("mm_copath_patient_list.csv"));
        accNoReader.readLine();
        String accNoReaderLine;
        while((accNoReaderLine = accNoReader.readLine()) != null) {
            String accNo = accNoReaderLine.split(",")[0];
            System.out.println(cpcf.getCoPathCaseByAccNo(accNo, pnrf));
        }
        accNoReader.close();
        
        connCoPath.close();
        connCdw.close();
        
    }
    
}
