package edu.emory.pathology.export4dj.data;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Geofrey H. Smith
 */
public class CoPathCase {
    
    public static class CoPathProcedure {
        
        public String procName;
        public String interp;

        public CoPathProcedure(ResultSet rs) throws SQLException {
            this.procName = rs.getString("proc_name");
            this.interp = rs.getString("procint_text")
                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
                .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
                .replace("\r", "")
                .replaceAll("\\s+$", "");
        }
        
    }

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
    public String specimenId;
    public String accNo;
    public Date accessionDate;
    public Date collectionDate;
    public String empi;
    public String finalDiagnosis;
    public List<CoPathProcedure> procedures;
    public Map<String, CoPathProcedure> procedureMap;

    public CoPathCase(ResultSet rs) throws SQLException {
        this.specimenId = rs.getString("specimen_id");
        this.accNo = rs.getString("specnum_formatted");
        this.accessionDate = rs.getDate("accession_date");
        this.collectionDate = rs.getDate("datetime_taken");
        this.empi = rs.getString("universal_mednum_stripped");
        this.finalDiagnosis = rs.getString("final_text")
            .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
            .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
            .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
            .replace("\r", "")
            .replaceAll("\\s+$", "");
    }

    @Override
    public String toString() {
        return(String.format(
            "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            specimenId.replace("'", "\""),
            accNo.replace("'", "\""),
            sdf.format(accessionDate),
            sdf.format(collectionDate),
            empi.replace("'", "\""),
            finalDiagnosis.replace("'", "\""),
            procedureMap.get("Flow Cytometry").interp,
            procedureMap.get("Chromosome Analysis").interp,
            procedureMap.get("Multiple Myeloma Panel, FISH").interp
        ));
    }
    
}
