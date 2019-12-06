package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.SebiaCaseFinder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

/**
 *
 * @author Geoffrey H. Smith
 */
public class AddSebiaNormalControlUtility {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, JAXBException {

        SebiaCaseFinder scf = new SebiaCaseFinder(new File(args[1]));
        
        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        Export4DJ export4DJ = (Export4DJ)unmarshaller.unmarshal(new FileInputStream(args[0]));

        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".xml", "") + ".sebia_normal.csv")));
        accNoWriter.println(CoPathCase.toStringHeader());

        for(CoPathCase cpc : export4DJ.coPathCases) {
            System.out.println(cpc.accNo);
            if(cpc.pathNetResults != null && cpc.pathNetResults.size() > 0) {
                if(cpc.getPathNetResultMap().get("SPEINTERP").accNo != null) {
                    cpc.sebiaCaseSerumNormalControl = scf.getSebiaNormalControlCaseByAccNo(cpc.getPathNetResultMap().get("SPEINTERP").accNo);
                }
                if(cpc.getPathNetResultMap().get("Urine Protein Electrophoresis").accNo != null) {
                    cpc.sebiaCaseUrineNormalControl = scf.getSebiaNormalControlCaseByAccNo(cpc.getPathNetResultMap().get("Urine Protein Electrophoresis").accNo);
                }
            }
            accNoWriter.println(cpc);
            accNoWriter.flush();
        }

        accNoWriter.close();
        
        Marshaller m = jc.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, new Boolean(true));
        m.marshal(export4DJ, new FileOutputStream(new File(args[0].replace(".xml", ".sebia_normal.xml"))));
        
    }
    
}
