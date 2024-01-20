package edu.emory.pathology.export4dj.data;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
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
        public String comment;

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
            this.comment = rs.getString("procres_text") == null ? null : rs.getString("procres_text")
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
        @XmlTransient
        public Boolean structuralAbnNeg;
        @XmlAttribute
        public Boolean copyNumberAbn;
        @XmlTransient
        public Boolean copyNumberAbnNeg;
        @XmlAttribute
        public Boolean amplificationAbn;
        @XmlTransient
        public Boolean amplificationAbnNeg;
        @XmlAttribute
        public Boolean otherAbn;
        @XmlTransient
        public Boolean otherAbnNeg;
        @XmlAttribute
        public String otherAbnName;

        public FishProbe() {
        }

        public FishProbe(ResultSet rs) throws SQLException {
            this.probeNumber = rs.getInt("probe_no");
            this.probeName = rs.getString("val_name");
        }
        
        public void setVariationProperties(ResultSet rs) throws SQLException {
            if(rs.getString("val").matches("(?).*RR[0-9]Pos(itive)?")) {
                structuralAbn = true;
            }
            else if(rs.getString("val").matches("(?).*CN[0-9]Pos(itive)?")) {
                copyNumberAbn = true;
            }
            else if(rs.getString("val").matches("(?).*AR[0-9]Pos(itive)?")) {
                amplificationAbn = true;
            }
            else if(rs.getString("val").matches("(?).*OR[0-9]Pos(itive)?")) {
                otherAbn = true;
                otherAbnName = rs.getString("val_freetext_char");
            }
            else if(rs.getString("val").matches("(?).*RR[0-9]Neg(ative)?")) {
                structuralAbnNeg = true;
            }
            else if(rs.getString("val").matches("(?).*CN[0-9]Neg(ative)?")) {
                copyNumberAbnNeg = true;
            }
            else if(rs.getString("val").matches("(?).*AR[0-9]Neg(ative)?")) {
                amplificationAbnNeg = true;
            }
            else if(rs.getString("val").matches("(?).*OR[0-9]Neg(ative)?")) {
                otherAbnNeg = true;
            }
        }

        @XmlAttribute
        public Boolean getAllNegative() {
            if(
                structuralAbnNeg != null && structuralAbnNeg
                && copyNumberAbnNeg != null && copyNumberAbnNeg
                && amplificationAbnNeg != null && amplificationAbnNeg
                && otherAbnNeg != null && otherAbnNeg
            ) {
                return true;
            }
            return null;
        }

        public void setAllNegative(Boolean allNegative) {
            if(allNegative) {
                structuralAbnNeg = true;
                copyNumberAbnNeg = true;
                amplificationAbnNeg = true;
                otherAbnNeg = true;
            }
        }
        
        public String getVariationConcatenated() {
            List<String> vc = new ArrayList<>();
            if(getAllNegative() != null && getAllNegative()) {
                return "negative";
            }
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
    public String fin;
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
    public String countsAndDiffs;
    public String karyotype;
    public Demographics demographics;
    @XmlElementWrapper(name = "procedures")
    @XmlElement(name = "procedure")
    public List<CoPathProcedure> procedures;
    @XmlTransient
    private Map<String, CoPathProcedure> procedureMap;
    @XmlElementWrapper(name = "fishProbes")
    @XmlElement(name = "fishProbe")
    public List<FishProbe> fishProbes;
    @XmlTransient
    private Map<Integer, FishProbe> fishProbeMap;
    @XmlElementWrapper(name = "pathNetResults")
    @XmlElement(name = "pathNetResult")
    public List<PathNetResult> pathNetResults;
    @XmlElementWrapper(name = "vitals")
    @XmlElement(name = "vital")
    public List<Vital> vitals;
    @XmlTransient
    private Map<String, PathNetResult> pathNetResultMap;
    @XmlTransient
    private Map<String, Vital> vitalMap;
    public SebiaCase sebiaCaseSerum;
    public SebiaCase sebiaCaseSerumGelControl;
    public SebiaCase sebiaCaseUrine;
    public SebiaCase sebiaCaseUrineGelControl;

    @XmlAttribute
    public String searchAccNo;
    @XmlAttribute
    public String searchPtNo;
    @XmlAttribute
    public String searchRecordId;
    @XmlTransient
    public Date searchDob;
    @XmlTransient
    public Date searchDateOfDx;
    public String diagnosisCd;
    public String diagnosisDesc;
    
    public CoPathCase() {
    }

    public CoPathCase(ResultSet rs) throws SQLException {
        this.fin = rs.getString("fin");
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
        this.countsAndDiffs = rs.getString("counts_and_diffs") == null ? null : rs.getString("counts_and_diffs")
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
        sb.append(String.format("\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            "searchAccNo",
            "searchPtNo",
            "searchRecordId",
            "searchDob",
            "searchDateOfDx",
            "diagnosisCd",
            "diagnosisDesc",
            "accNo",
            "accDate",
            "collDate",
            "empi",
            "birthDate",
            "deathDate",
            "latestDischargeDate",
            "ethnicity",
            "race",
            "ethnicGroup",
            "gender",
            "zip_code",
            "finalDiag",
            "countsAndDiffs",
            "flowInterp",
            "karyotype",
            "chromInterp",
            "fishInterp",
            "fishComment"
        ));
        for(int probeNumber = 1; probeNumber <= 7; probeNumber++) {
            sb.append(String.format(",\"%s\",\"%s\"",
                String.format("[MM]FISH%1d-probeName", probeNumber),
                String.format("[MM]FISH%1d-variation", probeNumber)
            ));
        }
        for(int probeNumber = 8; probeNumber <= 9; probeNumber++) {
            sb.append(String.format(",\"%s\",\"%s\"",
                String.format("[MM-Ex]FISH%1d-probeName", probeNumber),
                String.format("[MM-Ex]FISH%1d-variation", probeNumber)
            ));
        }
        for(int probeNumber = 10; probeNumber <=14; probeNumber++) {
            sb.append(String.format(",\"%s\",\"%s\"",
                String.format("[AML]FISH%1d-probeName", probeNumber),
                String.format("[AML]FISH%1d-variation", probeNumber)
            ));
        }
        for(int probeNumber = 15; probeNumber <=18; probeNumber++) {
            sb.append(String.format(",\"%s\",\"%s\"",
                String.format("[MDS]FISH%1d-probeName", probeNumber),
                String.format("[MDS]FISH%1d-variation", probeNumber)
            ));
        }
        for(int x = 1; x <= 38; x++) {
            sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                String.format("rslt%02d-resultName", x),
                String.format("rslt%02d-collectionDateDelta", x),
                String.format("rslt%02d-value", x),
                String.format("rslt%02d-uom", x),
                String.format("rslt%02d-flag", x),
                String.format("rslt%02d-interp", x)
            ));
        }
        for(int x = 1; x <= 8; x++) {
            sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\"",
                String.format("vital%02d-resultName", x),
                String.format("vital%02d-collectionDateDelta", x),
                String.format("vital%02d-value", x),
                String.format("vital%02d-uom", x)
            ));
        }
        sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
          "sebiaSerumCurve",
          "sebiaSerumFractions",
          "sebiaSerumGelControlCurve",
          "sebiaSerumGelControlFractions",
          "sebiaUrineCurve",
          "sebiaUrineFractions",
          "sebiaUrineGelControlCurve",
          "sebiaUrineGelControlFractions"
        ));
        return(sb.toString());
    }
    
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format(
            "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
            searchAccNo == null ? "" : searchAccNo,
            searchPtNo == null ? "" : searchPtNo,
            searchRecordId == null ? "" : searchRecordId,
            getSearchDob(),
            getSearchDateOfDx(),
            diagnosisCd == null ? "" : diagnosisCd,
            diagnosisDesc == null ? "" : diagnosisDesc,
            accNo.replace("\"", "'"),
            getAccessionDate(),
            getCollectionDate(),
            empi.replace("\"", "'"),
            demographics == null ? "" : demographics.getBirthDate(),
            demographics == null ? "" : demographics.getDeathDate(),
            demographics == null ? "" : demographics.getLatestDischargeDate(),
            demographics == null ? "" : demographics.ethnicity,
            demographics == null ? "" : demographics.race,
            demographics == null ? "" : demographics.ethnicGroup,
            demographics == null ? "" : demographics.gender,
            demographics == null ? "" : demographics.zipCode,
            finalDiagnosis == null ? "" : finalDiagnosis.replace("\"", "'"),
            countsAndDiffs == null ? "" : countsAndDiffs.replace("\"", "'"),
            getProcedureMap().get("Flow Cytometry") == null || getProcedureMap().get("Flow Cytometry").interp == null ? "" : getProcedureMap().get("Flow Cytometry").interp.replace("\"", "'"),
            karyotype == null ? "" : karyotype.replace("\"", "'"),
            getProcedureMap().get("Chromosome Analysis") == null || getProcedureMap().get("Chromosome Analysis").interp == null ? "" : getProcedureMap().get("Chromosome Analysis").interp.replace("\"", "'"),
            (getProcedureMap().get("Multiple Myeloma Panel, FISH") == null || getProcedureMap().get("Multiple Myeloma Panel, FISH").interp == null ? "" : "\n\n[MM Panel]\n\n" + getProcedureMap().get("Multiple Myeloma Panel, FISH").interp.replace("\"", "'"))
            + (getProcedureMap().get("t(4;14) and t(14;16) Panel, FISH") == null || getProcedureMap().get("t(4;14) and t(14;16) Panel, FISH").interp == null ? "" : "\n\n[MM Panel Extended]\n\n" + getProcedureMap().get("t(4;14) and t(14;16) Panel, FISH").interp.replace("\"", "'"))
            + (getProcedureMap().get("AML Panel, FISH") == null || getProcedureMap().get("AML Panel, FISH").interp == null ? "" : "\n\n[AML Panel]\n\n" + getProcedureMap().get("AML Panel, FISH").interp.replace("\"", "'"))
            + (getProcedureMap().get("MDS Panel, FISH") == null || getProcedureMap().get("MDS Panel, FISH").interp == null ? "" : "\n\n[MDS Panel]\n\n" + getProcedureMap().get("MDS Panel, FISH").interp.replace("\"", "'")),
            (getProcedureMap().get("Multiple Myeloma Panel, FISH") == null || getProcedureMap().get("Multiple Myeloma Panel, FISH").comment == null ? "" : "\n\n[MM Panel]\n\n" + getProcedureMap().get("Multiple Myeloma Panel, FISH").comment.replace("\"", "'"))
            + (getProcedureMap().get("t(4;14) and t(14;16) Panel, FISH") == null || getProcedureMap().get("t(4;14) and t(14;16) Panel, FISH").comment == null ? "" : "\n\n[MM Panel Extended]\n\n" + getProcedureMap().get("t(4;14) and t(14;16) Panel, FISH").comment.replace("\"", "'"))
            + (getProcedureMap().get("AML Panel, FISH") == null || getProcedureMap().get("AML Panel, FISH").comment == null ? "" : "\n\n[AML Panel]\n\n" + getProcedureMap().get("AML Panel, FISH").comment.replace("\"", "'"))
            + (getProcedureMap().get("MDS Panel, FISH") == null || getProcedureMap().get("MDS Panel, FISH").comment == null ? "" : "\n\n[MDS Panel]\n\n" + getProcedureMap().get("MDS Panel, FISH").comment.replace("\"", "'"))
        ));
        for(int probeNumber = 1; probeNumber <=18; probeNumber++) {
            sb.append(String.format(",\"%s\",\"%s\"",
                getFishProbeMap().get(probeNumber) == null ? "" : getFishProbeMap().get(probeNumber).probeName,
                getFishProbeMap().get(probeNumber) == null ? "" : getFishProbeMap().get(probeNumber).getVariationConcatenated()
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
        if(vitals != null) {
            for(Vital vital : vitals) {
                sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\"",
                    vital.resultName.replace("\"", "'"),
                    vital.recordedDateDelta == null ? "" : vital.recordedDateDelta.toString(),
                    vital.value == null ? "" : vital.value.replace("\"", "'"),
                    vital.uom == null ? "" : vital.uom.replace("\"", "'")
                ));
            }
        }
        sb.append(String.format(",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
          sebiaCaseSerum == null ? "" : sebiaCaseSerum.curve,
          sebiaCaseSerum == null || sebiaCaseSerum.sebiaFractions == null ? "" : sebiaCaseSerum.sebiaFractions,
          sebiaCaseSerumGelControl == null ? "" : sebiaCaseSerumGelControl.curve,
          sebiaCaseSerumGelControl == null || sebiaCaseSerumGelControl.sebiaFractions == null ? "" : sebiaCaseSerumGelControl.sebiaFractions,
          sebiaCaseUrine == null ? "" : sebiaCaseUrine.curve,
          sebiaCaseUrine == null || sebiaCaseUrine.sebiaFractions == null? "" : sebiaCaseUrine.sebiaFractions,
          sebiaCaseUrineGelControl == null ? "" : sebiaCaseUrineGelControl.curve,
          sebiaCaseUrineGelControl == null || sebiaCaseUrineGelControl.sebiaFractions == null? "" : sebiaCaseUrineGelControl.sebiaFractions
        ));
        return(sb.toString());
    }

    @XmlAttribute
    public String getAccessionDate() {
        return sdf.format(this.accessionDate);
    }

    public void setAccessionDate(String accessionDate) throws ParseException {
        this.accessionDate = new Date(sdf.parse(accessionDate).getTime());
    }

    @XmlAttribute
    public String getCollectionDate() {
        return sdf.format(this.collectionDate);
    }

    public void setCollectionDate(String collectionDate) throws ParseException {
        this.collectionDate = new Date(sdf.parse(collectionDate).getTime());
    }

    @XmlTransient
    public Map<String, CoPathProcedure> getProcedureMap() {
        if(procedureMap == null) {
            procedureMap = new HashMap<>();
            for(CoPathProcedure coPathProcedure : procedures) {
                procedureMap.put(coPathProcedure.procName, coPathProcedure);
            }
        }
        return procedureMap;
    }
    
    @XmlTransient
    public Map<Integer, FishProbe> getFishProbeMap() {
        if(fishProbeMap == null) {
            fishProbeMap = new HashMap<>();
            for(FishProbe fishProbe : fishProbes) {
                fishProbeMap.put(fishProbe.probeNumber, fishProbe);
            }
        }
        return fishProbeMap;
    }
    
    @XmlTransient
    public Map<String, PathNetResult> getPathNetResultMap() {
        if(pathNetResultMap == null) {
            pathNetResultMap = new HashMap<>();
            for(PathNetResult pathNetResult : pathNetResults) {
                pathNetResultMap.put(pathNetResult.resultName, pathNetResult);
            }
        }
        return pathNetResultMap;
    }

    @XmlTransient
    public Map<String, Vital> getVitalMap() {
        if(vitalMap == null) {
            vitalMap = new HashMap<>();
            for(Vital vital : vitals) {
                vitalMap.put(vital.resultName, vital);
            }
        }
        return vitalMap;
    }

    @XmlAttribute
    public String getSearchDob() {
        if(searchDob == null) {
            return "";
        }
        return sdf.format(this.searchDob);
    }

    public void setSearchDob(String searchDob) throws ParseException {
        if(searchDob != null && searchDob.length() > 0) {
            this.searchDob = new Date(sdf.parse(searchDob).getTime());
        }
    }
    
    @XmlAttribute
    public String getSearchDateOfDx() {
        if(searchDateOfDx == null) {
            return "";
        }
        return sdf.format(this.searchDateOfDx);
    }

    public void setSearchDateOfDx(String searchDateOfDx) throws ParseException {
        if(searchDateOfDx != null && searchDateOfDx.length() > 0) {
            this.searchDateOfDx = new Date(sdf.parse(searchDateOfDx).getTime());
        }
    }

    
}
