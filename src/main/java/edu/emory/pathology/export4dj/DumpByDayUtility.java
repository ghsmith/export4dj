package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.data.SebiaCase;
import edu.emory.pathology.export4dj.finder.SebiaCaseFinder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.xml.bind.JAXBException;

/**
 *
 * @author Geoffrey H. Smith
 */
public class DumpByDayUtility {


    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, JAXBException {

        Properties priv = new Properties();
        try(InputStream inputStream = DumpByDayUtility.class.getClassLoader().getResourceAsStream("private.properties")) {
            priv.load(inputStream);
        }
        
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connCdw = DriverManager.getConnection(priv.getProperty("connCdw.url"), priv.getProperty("connCdw.u"), priv.getProperty("connCdw.p"));
        connCdw.setAutoCommit(false);
        connCdw.createStatement().execute("set role hnam_sel_all");
        
        PreparedStatement pstmt1 = connCdw.prepareStatement(
            "select "
          + "  ce.result_val "
          + "from "
          + "  hnamdwh.clinical_event ce "
          + "  join hnamdwh.accession_order_r aor on(aor.order_id = ce.order_id) "
          + "  join hnamdwh.accession a on (a.accession_id = aor.accession_id) "
          + "where "
          + "  ce.task_assay_cd = 485880674 "
          + "  and ce.valid_until_dt_tm = '31-DEC-2100' "
          + "  and a.accession = ? "
        );                

        SebiaCaseFinder scf = new SebiaCaseFinder(new File(args[0]));
        
        Export4DJ export4DJ = new Export4DJ();
        export4DJ.coPathCases = new ArrayList<>();

        System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
            "gelKey",
            "id",
            "longId",
            "paraproteinConc",
            "curve",
            "control-gelKey",
            "control-id",
            "control-curve"
        ));
        
        List<SebiaCase> sebiaCases = scf.getSebiaCasesByDate(args[1]);

        for(SebiaCase sc : sebiaCases) {

            System.err.println(sc.id);
            
            if(!sc.id.matches("[0-9]*[A-Z]")) {
                System.err.println("...skipping");
                continue;
            }
            
            SebiaCase scControl = scf.getSebiaGelControlByGelKey(sc.gelKey);

            String longId = String.format("00%s201%s%s0%s", sc.id.substring(0,3), sc.id.substring(3,4), sc.id.substring(4,7), sc.id.substring(7,12));
            pstmt1.setString(1, longId);
            ResultSet rs1 = pstmt1.executeQuery();
            rs1.next();
            String paraproteinConcentration = rs1.getString(1);
            rs1.close();
            
            System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
                sc.gelKey,
                sc.id,
                longId,
                paraproteinConcentration,
                sc.curve,
                scControl.gelKey,
                scControl.id,
                scControl.curve
            ));

        }

        connCdw.close();

    }
    
}
