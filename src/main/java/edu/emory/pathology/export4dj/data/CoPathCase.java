package edu.emory.pathology.export4dj.data;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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

    @XmlRootElement
    public static class FishProbe {
    
        @XmlAttribute
        public Integer probeNumber;
        @XmlAttribute
        public String probeName;
        @XmlAttribute
        public Boolean structuralAbn;
        @XmlAttribute
        public Boolean copyNumberAbn;
        @XmlAttribute
        public Boolean amplificationAbn;
        @XmlAttribute
        public Boolean otherAbn;
        @XmlAttribute
        public String otherAbnName;

        public FishProbe() {
        }

        public FishProbe(ResultSet rs) throws SQLException {
            this.probeNumber = rs.getInt("probe_no");
            this.probeName = rs.getString("val_name");
        }
        
        public void setVariationProperties(ResultSet rs) throws SQLException {
            if(rs.getString("val").matches(".*RR[0-9]Positive")) {
                structuralAbn = true;
            }
            else if(rs.getString("val").matches(".*CN[0-9]Positive")) {
                copyNumberAbn = true;
            }
            else if(rs.getString("val").matches(".*AR[0-9]Positive")) {
                amplificationAbn = true;
            }
            else if(rs.getString("val").matches(".*OR[0-9]Positive")) {
                otherAbn = true;
                otherAbnName = rs.getString("val_freetext_char");
            }
        }
        
        public String getVariationConcatenated() {
            List<String> vc = new ArrayList<>();
            if(structuralAbn != null && structuralAbn) {
                vc.add("struct");
            }
            if(copyNumberAbn != null && copyNumberAbn) {
                vc.add("CNA");
            }
            if(amplificationAbn != null && amplificationAbn) {
                vc.add("amp");
            }
            if(otherAbn != null && otherAbn) {
                vc.add(String.format("other[%s]", otherAbnName));
            }
            return String.join(",", vc);
        }
        
    }
    
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
    @XmlTransient
    public String specimenId;
    @XmlAttribute
    public String accNo;
    @XmlTransient
    public Date accessionDate;
    @XmlTransient
    public Date collectionDate;
    @XmlAttribute
    public String empi;
    public String finalDiagnosis;
    public String karyotype;
    @XmlElementWrapper(name = "procedures")
    @XmlElement(name = "procedure")
    public List<CoPathProcedure> procedures;
    @XmlTransient
    public Map<String, CoPathProcedure> procedureMap;
    @XmlElementWrapper(name = "fishProbes")
    @XmlElement(name = "fishProbe")
    public List<FishProbe> fishProbes;
    @XmlTransient
    public Map<Integer, FishProbe> fishProbeMap;
    @XmlElementWrapper(name = "pathNetResults")
    @XmlElement(name = "pathNetResult")
    public List<PathNetResult> pathNetResults;
    @XmlTransient
    public Map<String, PathNetResult> pathNetResultMap;
    public SebiaCase sebiaCaseSerum;
    public SebiaCase sebiaCaseUrine;

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
        this.karyotype = rs.getString("karyotype") == null ? null : rs.getString("karyotype")
            .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
            .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
            .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
            .replace("\r", "")
            .replaceAll("\\s+$", "");
    }

    public static String toStringHeader() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            "accNo",
            "accDate",
            "collDate",
            "empi",
            "finalDiag",
            "flowInterp",
            "karyotype",
            "chromInterp",
            "fishInterp"
        ));
        for(int probeNumber = 1; probeNumber <=9; probeNumber++) {
            sb.append(String.format(",\"%s\",\"%s\"",
                String.format("FISH%1d-probeName", probeNumber),
                String.format("FISH%1d-variation", probeNumber)
            ));
        }
        for(int x = 1; x <= 25; x++) {
            sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                String.format("rslt%02d-resultName", x),
                String.format("rslt%02d-collectionDateDelta", x),
                String.format("rslt%02d-value", x),
                String.format("rslt%02d-uom", x),
                String.format("rslt%02d-flag", x),
                String.format("rslt%02d-interp", x)
            ));
        }
        sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\"",
          "sebiaSerumCurve",
          "sebiaSerumFractions",
          "sebiaUrineCurve",
          "sebiaUrineFractions"
        ));
        return(sb.toString());
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format(
            "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            accNo.replace("\"", "'"),
            sdf.format(accessionDate),
            sdf.format(collectionDate),
            empi.replace("\"", "'"),
            finalDiagnosis == null ? "" : finalDiagnosis.replace("\"", "'"),
            procedureMap.get("Flow Cytometry") == null || procedureMap.get("Flow Cytometry").interp == null ? "" : procedureMap.get("Flow Cytometry").interp.replace("\"", "'"),
            karyotype == null ? "" : karyotype.replace("\"", "'"),
            procedureMap.get("Chromosome Analysis") == null || procedureMap.get("Chromosome Analysis").interp == null ? "" : procedureMap.get("Chromosome Analysis").interp.replace("\"", "'"),
            (procedureMap.get("Multiple Myeloma Panel, FISH") == null || procedureMap.get("Multiple Myeloma Panel, FISH").interp == null ? "" : procedureMap.get("Multiple Myeloma Panel, FISH").interp.replace("\"", "'"))
            + (procedureMap.get("t(4;14) and t(14;16) Panel, FISH") == null || procedureMap.get("t(4;14) and t(14;16) Panel, FISH").interp == null ? "" : "\n\n[Additional Probes]\n\n" + procedureMap.get("t(4;14) and t(14;16) Panel, FISH").interp.replace("\"", "'"))
        ));
        for(int probeNumber = 1; probeNumber <=9; probeNumber++) {
            sb.append(String.format(",\"%s\",\"%s\"",
                fishProbeMap.get(probeNumber) == null ? "" : fishProbeMap.get(probeNumber).probeName,
                fishProbeMap.get(probeNumber) == null ? "" : fishProbeMap.get(probeNumber).getVariationConcatenated()
            ));
        }
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
        sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\"",
          sebiaCaseSerum == null ? "" : sebiaCaseSerum.curve,
          sebiaCaseSerum == null || sebiaCaseSerum.sebiaFractions == null ? "" : sebiaCaseSerum.sebiaFractions,
          sebiaCaseUrine == null ? "" : sebiaCaseUrine.curve,
          sebiaCaseUrine == null || sebiaCaseUrine.sebiaFractions == null? "" : sebiaCaseUrine.sebiaFractions
        ));
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
