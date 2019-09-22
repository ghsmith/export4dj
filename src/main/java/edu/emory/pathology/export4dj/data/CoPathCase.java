package edu.emory.pathology.export4dj.data;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author Geofrey H. Smith
 */
@XmlRootElement
public class CoPathCase {
    
    @XmlRootElement
    public static class CoPathProcedure {
    
        @XmlAttribute
        public String procName;
        //@XmlAttribute
        public String interp;

        public CoPathProcedure() {
        }

        public CoPathProcedure(ResultSet rs) throws SQLException {
            this.procName = rs.getString("proc_name");
            this.interp = rs.getString("procint_text") == null ? null : rs.getString("procint_text")
                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
                .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
                .replace("\r", "")
                .replaceAll("\\s+$", "");
        }
        
    }

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
    public String specimenId;
    @XmlAttribute
    public String accNo;
    @XmlTransient
    public Date accessionDate;
    @XmlTransient
    public Date collectionDate;
    @XmlAttribute
    public String empi;
    //@XmlAttribute
    public String finalDiagnosis;
    @XmlElementWrapper(name = "procedures")
    @XmlElement(name = "procedure")
    public List<CoPathProcedure> procedures;
    @XmlTransient
    public Map<String, CoPathProcedure> procedureMap;
    @XmlElementWrapper(name = "pathNetResults")
    @XmlElement(name = "pathNetResult")
    public List<PathNetResult> pathNetResults;
    @XmlTransient
    public Map<String, PathNetResult> pathNetResultMap;

    public CoPathCase() {
    }

    public CoPathCase(ResultSet rs) throws SQLException {
        this.specimenId = rs.getString("specimen_id");
        this.accNo = rs.getString("specnum_formatted");
        this.accessionDate = rs.getDate("accession_date");
        this.collectionDate = rs.getDate("datetime_taken");
        this.empi = rs.getString("universal_mednum_stripped");
        this.finalDiagnosis = rs.getString("final_text") == null ? null : rs.getString("final_text")
            .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
            .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
            .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
            .replace("\r", "")
            .replaceAll("\\s+$", "");
    }

    public static String toStringHeader() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            "accNo",
            "accDate",
            "collDate",
            "empi",
            "finalDiag",
            "flowInterp",
            "chromInterp",
            "fishInterp"
        ));
        for(int x = 1; x <= 20; x++) {
            sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                String.format("resultName-%02d", x),
                String.format("collectionDateDelta-%02d", x),
                String.format("value-%02d", x),
                String.format("uom-%02d", x),
                String.format("flag-%02d", x),
                String.format("interp-%02d", x)
            ));
        }
        return(sb.toString());
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format(
            "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            accNo.replace("\"", "'"),
            sdf.format(accessionDate),
            sdf.format(collectionDate),
            empi.replace("\"", "'"),
            finalDiagnosis == null ? "" : finalDiagnosis.replace("\"", "'"),
            procedureMap.get("Flow Cytometry") == null || procedureMap.get("Flow Cytometry").interp == null ? "" : procedureMap.get("Flow Cytometry").interp.replace("\"", "'"),
            procedureMap.get("Chromosome Analysis") == null || procedureMap.get("Chromosome Analysis").interp == null ? "" : procedureMap.get("Chromosome Analysis").interp.replace("\"", "'"),
            procedureMap.get("Multiple Myeloma Panel, FISH") == null || procedureMap.get("Multiple Myeloma Panel, FISH").interp == null ? "" : procedureMap.get("Multiple Myeloma Panel, FISH").interp.replace("\"", "'")
        ));
        for(PathNetResult pathNetResult : pathNetResults) {
            sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                pathNetResult.resultName.replace("\"", "'"),
                pathNetResult.collectionDateDelta == null ? "" : pathNetResult.collectionDateDelta.toString(),
                pathNetResult.value == null ? "" : pathNetResult.value.replace("\"", "'"),
                pathNetResult.uom == null ? "" : pathNetResult.uom.replace("\"", "'"),
                pathNetResult.flag == null ? "" : pathNetResult.flag.replace("\"", "'"),
                pathNetResult.interp == null ? "" : pathNetResult.interp.replace("\"", "'")
            ));
        }
        return(sb.toString());
    }

    @XmlAttribute
    public String getAccessionDate() {
        return sdf.format(accessionDate);
    }
    @XmlAttribute
    public String getCollectionDate() {
        return sdf.format(collectionDate);
    }
    
}
