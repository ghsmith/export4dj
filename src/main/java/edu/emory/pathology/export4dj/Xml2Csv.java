package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.ParseException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

/**
 *
 * @author Geoffrey H. Smith
 */
public class Xml2Csv {
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException, JAXBException, ParseException {

        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        Export4DJ export4DJ = (Export4DJ)unmarshaller.unmarshal(new FileInputStream(args[0]));
        System.out.println(export4DJ.coPathCases.size() + " loaded");
        jc.generateSchema(new SchemaOutputResolver() {
            public Result createOutput(String namespaceURI, String suggestedFileName) throws IOException {
                File file = new File("export4dj.xsd");
                StreamResult result = new StreamResult(file);
                result.setSystemId(file.toURI().toURL().toString());
                return result;
            }                
        });

        PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".xml.new", ".csv"))));
        accNoWriter.println(CoPathCase.toStringHeader());
        for(CoPathCase coPathCase : export4DJ.coPathCases) {
            accNoWriter.println(coPathCase);
        }
        accNoWriter.close();
        
    }
    
}
