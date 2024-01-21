package edu.emory.pathology.export4dj;

import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.Export4DJ;
import edu.emory.pathology.export4dj.finder.CoPathCaseFinder;
import edu.emory.pathology.export4dj.finder.DemographicsFinder;
import edu.emory.pathology.export4dj.finder.PathNetResultFinder;
import edu.emory.pathology.export4dj.finder.SebiaCaseFinder;
import edu.emory.pathology.export4dj.finder.VitalFinder;
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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

        PreparedStatement pstmtDx = connCdw.prepareStatement("select diagnosis_primary_hosp_cd, (select diagnosis_desc from ehcvw.lkp_diagnosis where diagnosis_key = redhp.diagnosis_key) x  from ehcvw.lkp_encounter le join ehcvw.rel_encounter_dx_hosp_primary redhp on(le.encounter_key = redhp.encounter_extension_key) where encounter_nbr = ?");
        
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection connCoPath = DriverManager.getConnection(priv.getProperty("connCoPath.url"));

        CoPathCaseFinder cpcf = new CoPathCaseFinder(connCoPath);
        PathNetResultFinder pnrf = new PathNetResultFinder(connCdw);
        DemographicsFinder df = new DemographicsFinder(connCdw, connCoPath);
        VitalFinder vf = new VitalFinder(connCdw);

        //SebiaCaseFinder scf = new SebiaCaseFinder(new File(args[1]));
        SebiaCaseFinder scf = null;
        
        // new
        //Export4DJ export4DJ = new Export4DJ();
        //export4DJ.coPathCases = new ArrayList<>();
        
        //pickup where we left off
        Export4DJ export4DJ = null;
        {
            JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
            Unmarshaller unmarshaller = jc.createUnmarshaller();
            export4DJ = (Export4DJ)unmarshaller.unmarshal(new FileInputStream("latest_xml_prepped/" + args[0] + ".export4dj.xml"));
            System.out.println(export4DJ.coPathCases.size() + " loaded");
            // remove any cases that were definitely in-flight at the time of the last crash (not perfect)
            List<CoPathCase> keep = new ArrayList<CoPathCase>();
            for(CoPathCase candidateCoPathCase : export4DJ.coPathCases) {
                if(candidateCoPathCase.searchPtNo != null) { keep.add(candidateCoPathCase); }
            }
            export4DJ.coPathCases = keep;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

        Pattern p = Pattern.compile("([0-9]+)/([0-9]+)/([0-9]+)");
        
        BufferedReader mrnReader = new BufferedReader(new FileReader(args[0]));
        //String mrnHeaders = mrnReader.readLine();
        //PrintWriter accNoWriter = new PrintWriter(new FileWriter(new File(args[0].replace(".csv", "") + ".export4dj.csv")));
        //accNoWriter.println(CoPathCase.toStringHeader());
        int x = 0;
        try {
            String mrnReaderLine;
            while((mrnReaderLine = mrnReader.readLine()) != null) {
                System.out.println(mrnReaderLine);
                boolean skip = false;
                for(CoPathCase candidateCoPathCase : export4DJ.coPathCases) {
                    if(candidateCoPathCase.searchPtNo.equals(mrnReaderLine.split(",")[0])) {
                        //accNoWriter.println(candidateCoPathCase);
                        //accNoWriter.flush();
                        skip = true;
                        break;
                    }
                }
                if(skip) { continue; }

                String mrn = mrnReaderLine.split(",")[1];
                Date dob = null;
                if(mrnReaderLine.split(",").length >= 3 && mrnReaderLine.split(",")[2] != null && mrnReaderLine.split(",")[2].length() > 0) {
                    Matcher m = p.matcher(mrnReaderLine.split(",")[2]);
                    if(m.matches()) {
                        if(m.group(3).length() == 4) {
                            dob = new Date(sdf.parse(mrnReaderLine.split(",")[2]).getTime());
                        }
                        else if(m.group(3).length() == 2) {
                            dob = new Date(sdf.parse(m.group(1) + "/" + m.group(2) + "/19" + m.group(3)).getTime());
                        }
                        else {
                            throw new RuntimeException("bad DOB");
                        }
                    }
                    else {
                        throw new RuntimeException("bad DOB");
                    }
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
                    System.out.print(coPathCase.accNo);
                    coPathCase.searchPtNo = mrnReaderLine.split(",")[0];
                    coPathCase.searchRecordId = mrnReaderLine.split(",")[1];
                    coPathCase.searchDob = dob;
                    pstmtDx.setString(1, coPathCase.fin);
                    System.out.print(".");
                    ResultSet rsDx = pstmtDx.executeQuery();
                    if(rsDx.next()) {
                        coPathCase.diagnosisCd = rsDx.getString(1);
                        coPathCase.diagnosisDesc = rsDx.getString(2);
                    }
                    if(mrnReaderLine.split(",").length >= 4 && mrnReaderLine.split(",")[3] != null && mrnReaderLine.split(",")[3].length() > 0) {
                        coPathCase.searchDateOfDx = new Date(sdf.parse(mrnReaderLine.split(",")[3]).getTime());
                    }
                    System.out.print(".");
                    coPathCase.demographics = df.getDemographicsByEmpiAndAccNo(coPathCase.empi, coPathCase.accNo);
                    System.out.print(".");
                    coPathCase.vitals = vf.getVitalsByEmpiProximateToCollectionDate(coPathCase.empi, coPathCase.collectionDate);
                    System.out.println(".");
                    //accNoWriter.println(coPathCase);
                }
                //accNoWriter.flush();
                if(!coPathCases.isEmpty()) {
                    x++;
                    if(x % 20 == 0) {
                        JAXBContext jc = JAXBContext.newInstance(new Class[] { Export4DJ.class });
                        Marshaller m = jc.createMarshaller();
                        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, new Boolean(true));
                        m.marshal(export4DJ, new FileOutputStream(new File(args[0].replace(".csv", "") + ".export4dj.xml")));
                    }
                }
            }
            mrnReader.close();
            //accNoWriter.close();

            connCoPath.close();
            connCdw.close();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        
        {
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
    
}
