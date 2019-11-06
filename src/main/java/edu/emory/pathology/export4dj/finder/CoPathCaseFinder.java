package edu.emory.pathology.export4dj.finder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.CoPathCase.CoPathProcedure;
import edu.emory.pathology.export4dj.data.CoPathCase.FishProbe;
import edu.emory.pathology.export4dj.data.PathNetResult;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author Geoffrey H. Smith
 */
public class CoPathCaseFinder {
    
    private Connection conn;
    private PreparedStatement pstmt1;
    private PreparedStatement pstmt2;
    private PreparedStatement pstmt3;
    private PreparedStatement pstmt4;

    public CoPathCaseFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt1 = conn.prepareStatement(
            "select top 1                                                                                                                     "
          + "  c_specimen.specimen_id,                                                                                                        "
          + "  c_specimen.specnum_formatted,                                                                                                  "
          + "  c_specimen.accession_date,                                                                                                     "
          + "  p_part.datetime_taken,                                                                                                         "
          + "  r_pat_demograph.universal_mednum_stripped,                                                                                     "
          + "  (select text_data from c_spec_text where specimen_id = c_specimen.specimen_id and texttype_id = '$final') as final_text,       "
          + "  (select text_data from c_spec_text where specimen_id = c_specimen.specimen_id and texttype_id = 'emy41'                        "
          + "   and (select count(*) from c_spec_text where specimen_id = c_specimen.specimen_id and texttype_id = 'emy41') = 1) as karyotype "
          + "from                                                                                                                             "
          + "  c_specimen                                                                                                                     "
          + "  join r_pat_demograph on (r_pat_demograph.patdemog_id = c_specimen.patdemog_id)                                                 "
          + "  join p_part on (p_part.specimen_id = c_specimen.specimen_id)                                                                   "
          + "where                                                                                                                            "
          + "  c_specimen.specnum_formatted = ?                                                                                               "
        );
        pstmt2 = conn.prepareStatement(
            "select                                                                                                                                                                   "
          + "  c_d_sprotype.name proc_name,                                                                                                                                           "
          + "  (select text_data from c_spec_text where specimen_id = p_special_proc.specimen_id and link_inst = p_special_proc.sp_inst and texttype_id = '$procint') as procint_text "
          + "from                                                                                                                                                                     "
          + "  p_special_proc                                                                                                                                                         "
          + "  join c_d_sprotype on (c_d_sprotype.id = p_special_proc.sprotype_id)                                                                                                    "
          + "where                                                                                                                                                                    "
          + "  p_special_proc.specimen_id = ?                                                                                                                                         "
          + "order by                                                                                                                                                                 "
          + "  p_special_proc.sp_inst                                                                                                                                                 "
        );
        pstmt3 = conn.prepareStatement(
            "select                                                                                                   "
          + "  cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                         "
          + "  cast(ssd.inst as int) as item_no,                                                                      "
          + "  dsc.abbr as item,                                                                                      "
          + "  dsc.name as item_name,                                                                                 "
          + "  dsv.abbr as val,                                                                                       "
          + "  dsv.name as val_name,                                                                                  "
          + "  dsv.fillin_type as val_freetext_type,                                                                  "
          + "  ssd.fillin_char as val_freetext_char                                                                   "
          + "from                                                                                                     "
          + "  c_spec_synoptic_ws ssw                                                                                 "
          + "  join c_spec_synoptic_dx ssd on(ssd.specimen_id = ssw.specimen_id and ssd.worksheet_inst = ssw.ws_inst) "
          + "  join c_d_synoptic_value dsv on(dsv.id = ssd.synoptic_value_id)                                         "
          + "  join c_d_synoptic_category dsc on(dsc.id = dsv.category_id)                                            "
          + "where                                                                                                    "
          + "  dsc.name like 'Probe Set (_)'                                                                          "
          + "  and ssw.worksheet_id = 'temy85'                                                                        "
          + "  and ssw.specimen_id = ?                                                                                "
          + "order by                                                                                                 "
          + "  2                                                                                                      "
        );
        pstmt4 = conn.prepareStatement(
            "select                                                                                                   "
          + "  cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                         "
          + "  cast(ssd.inst as int) as item_no,                                                                      "
          + "  dsc.abbr as item,                                                                                      "
          + "  dsc.name as item_name,                                                                                 "
          + "  dsv.abbr as val,                                                                                       "
          + "  dsv.name as val_name,                                                                                  "
          + "  dsv.fillin_type as val_freetext_type,                                                                  "
          + "  ssd.fillin_char as val_freetext_char                                                                   "
          + "from                                                                                                     "
          + "  c_spec_synoptic_ws ssw                                                                                 "
          + "  join c_spec_synoptic_dx ssd on(ssd.specimen_id = ssw.specimen_id and ssd.worksheet_inst = ssw.ws_inst) "
          + "  join c_d_synoptic_value dsv on(dsv.id = ssd.synoptic_value_id)                                         "
          + "  join c_d_synoptic_category dsc on(dsc.id = dsv.category_id)                                            "
          + "where                                                                                                    "
          + "  (                                                                                                      "
          + "    (                                                                                                    "
          + "      dsc.name like 'Rearrangement Results (_)'                                                          "
          + "      or dsc.name like 'Copy Number Results (_)'                                                         "
          + "      or dsc.name like 'Amplification Results (_)'                                                       "
          + "      or dsc.name like 'Other Results (_)'                                                               "
          + "    )                                                                                                    "
          + "    and                                                                                                  "
          + "    (                                                                                                    "
          + "      dsv.abbr like 'CF[_]Misc[_]RR_Positive'                                                            "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Positive'                                                         "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Positive'                                                         "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Positive'                                                         "
          + "    )                                                                                                    "
          + "  )                                                                                                      "
          + "  and ssw.worksheet_id = 'temy85'                                                                        "
          + "  and ssw.specimen_id = ?                                                                                "
          + "  and cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) = ?                              "
          + "order by                                                                                                 "
          + "  2                                                                                                      "
        );
    }
    
    public CoPathCase getCoPathCaseByAccNo(String accNo, PathNetResultFinder pathNetResultFinder, SebiaCaseFinder sebiaCaseFinder) throws SQLException {
        CoPathCase coPathCase = null;
        pstmt1.setString(1, accNo);
        ResultSet rs1 = pstmt1.executeQuery();
        if(rs1.next()) {
            coPathCase = new CoPathCase(rs1);
            coPathCase.procedures = new ArrayList<>();
            pstmt2.setString(1, coPathCase.specimenId);
            ResultSet rs2 = pstmt2.executeQuery();
            while(rs2.next()) {
                coPathCase.procedures.add(new CoPathProcedure(rs2));
            }
            rs2.close();
            coPathCase.fishProbes = new ArrayList<>();
            pstmt3.setString(1, coPathCase.specimenId);
            ResultSet rs3 = pstmt3.executeQuery();
            while(rs3.next()) {
                FishProbe fishProbe = new CoPathCase.FishProbe(rs3);
                coPathCase.fishProbes.add(fishProbe);
                pstmt4.setString(1, coPathCase.specimenId);
                pstmt4.setInt(2, fishProbe.probeNumber);
                ResultSet rs4 = pstmt4.executeQuery();
                while(rs4.next()) {
                    fishProbe.setVariationProperties(rs4);
                }
                rs4.close();
            }
            rs3.close();
            coPathCase.pathNetResults = pathNetResultFinder.getPathNetResultsByEmpiProximateToCollectionDate(coPathCase.empi, coPathCase.collectionDate);
            coPathCase.procedureMap = new HashMap<>();
            for(CoPathProcedure coPathProcedure : coPathCase.procedures) {
                coPathCase.procedureMap.put(coPathProcedure.procName, coPathProcedure);
            }
            coPathCase.fishProbeMap = new HashMap<>();
            for(FishProbe fishProbe : coPathCase.fishProbes) {
                coPathCase.fishProbeMap.put(fishProbe.probeNumber, fishProbe);
            }
            coPathCase.pathNetResultMap = new HashMap<>();
            for(PathNetResult pathNetResult : coPathCase.pathNetResults) {
                coPathCase.pathNetResultMap.put(pathNetResult.resultName, pathNetResult);
            }
            if(coPathCase.pathNetResultMap.get("SPEINTERP") != null && coPathCase.pathNetResultMap.get("SPEINTERP").accNo != null) {
                coPathCase.sebiaCaseSerum = sebiaCaseFinder.getSebiaCaseByAccNo(coPathCase.pathNetResultMap.get("SPEINTERP").accNo);
            }
            if(coPathCase.pathNetResultMap.get("Urine Protein Electrophoresis") != null && coPathCase.pathNetResultMap.get("Urine Protein Electrophoresis").accNo != null) {
                coPathCase.sebiaCaseUrine = sebiaCaseFinder.getSebiaCaseByAccNo(coPathCase.pathNetResultMap.get("Urine Protein Electrophoresis").accNo);
            }
        }
        rs1.close();
        return coPathCase;
    }
    
}
