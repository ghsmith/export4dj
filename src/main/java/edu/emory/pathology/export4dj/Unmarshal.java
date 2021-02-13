package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 *
 * @author Geoffrey H. Smith
 */
public class Unmarshal {
    
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

        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        Export4DJ export4DJ = (Export4DJ)unmarshaller.unmarshal(new FileInputStream(args[0]));
        System.out.println(export4DJ.coPathCases.size() + " loaded");

        PreparedStatement pstmtEnc = connCoPath.prepareStatement("select r_encounter.encounter_num from c_specimen join r_encounter on (c_specimen.encounter_id = r_encounter.encounter_id) where c_specimen.specnum_formatted = ?");
        PreparedStatement pstmtDx = connCdw.prepareStatement("select diagnosis_primary_hosp_cd, (select diagnosis_desc from ehcvw.lkp_diagnosis where diagnosis_key = redhp.diagnosis_key) x  from ehcvw.lkp_encounter le join ehcvw.rel_encounter_dx_hosp_primary redhp on(le.encounter_key = redhp.encounter_extension_key) where encounter_nbr = ?");
        
        for(CoPathCase coPathCase : export4DJ.coPathCases) {
            String enc = null;
            String dx = null;
            String desc = null;
            pstmtEnc.setString(1, coPathCase.accNo);
            ResultSet rsEnc = pstmtEnc.executeQuery();
            if(rsEnc.next()) {
                enc = rsEnc.getString(1);
            }
            rsEnc.close();
            if(enc != null && enc.length() > 0) {
                pstmtDx.setString(1, enc);
                ResultSet rsDx = pstmtDx.executeQuery();
                if(rsDx.next()) {
                    dx = rsDx.getString(1);
                    desc = rsDx.getString(2);
                }
                rsDx.close();
            }
            
            System.out.println(String.format("%-15s %-15s %-10s %s", coPathCase.accNo, enc, dx, desc));
            coPathCase.diagnosisCd = dx;
            coPathCase.diagnosisDesc = desc;
        }

        connCdw.close();
        connCoPath.close();
        
        Marshaller m = jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, new Boolean(true));
        m.marshal(export4DJ, new FileOutputStream(new File(args[0].replace(".export4dj.xml", ".diagnosis.export4dj.xml"))));
        
    }
    
}
