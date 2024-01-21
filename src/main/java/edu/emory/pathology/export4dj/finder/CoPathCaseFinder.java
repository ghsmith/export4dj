package edu.emory.pathology.export4dj.finder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.CoPathCase.CoPathProcedure;
import edu.emory.pathology.export4dj.data.CoPathCase.FishProbe;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Geoffrey H. Smith
 */
public class CoPathCaseFinder {
    
    private final Connection conn;
    private final PreparedStatement pstmt0;
    private final PreparedStatement pstmt0_noDob;
    private final PreparedStatement pstmt1;
    private final PreparedStatement pstmt2;
    private final PreparedStatement pstmt3;
    private final PreparedStatement pstmt4;

    public CoPathCaseFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt0 = conn.prepareStatement(
" select distinct " +
"   c_specimen.specnum_formatted, c_specimen_accession_date " +
" from " +
"   c_specimen " +
"   join r_medrec on(r_medrec.patdemog_id = c_specimen.patdemog_id) " +
"   join r_pat_demograph on(r_pat_demograph.patdemog_id = r_medrec.patdemog_id) " +
" where " +
"   r_medrec.medrec_num_stripped = ? " +
"   and r_pat_demograph.date_of_birth = ? " +
" order by " +
"   c_specimen.accession_date "
        );
        pstmt0_noDob = conn.prepareStatement(
" select distinct " +
"   c_specimen.specnum_formatted, c_specimen_accession_date " +
" from " +
"   c_specimen " +
"   join r_medrec on(r_medrec.patdemog_id = c_specimen.patdemog_id) " +
"   join r_pat_demograph on(r_pat_demograph.patdemog_id = r_medrec.patdemog_id) " +
" where " +
"   r_medrec.medrec_num_stripped = ? " +
" order by " +
"   c_specimen.accession_date "
        );
        pstmt1 = conn.prepareStatement(
            "select top 1                                                                                                                     "
          + "  (select r_encounter.encounter_num from r_encounter where r_encounter.encounter_id = c_specimen.encounter_id) fin,              "
          + "  c_specimen.specimen_id,                                                                                                        "
          + "  c_specimen.specnum_formatted,                                                                                                  "
          + "  c_specimen.accession_date,                                                                                                     "
          + "  p_part.datetime_taken,                                                                                                         "
          + "  r_pat_demograph.universal_mednum_stripped,                                                                                     "
          + "  (select text_data from c_spec_text where specimen_id = c_specimen.specimen_id and texttype_id = '$final') as final_text,       "
          + "  (select text_data from c_spec_text where specimen_id = c_specimen.specimen_id and texttype_id = 'emy7') as counts_and_diffs, "
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
            "select                                                                                                                                                                    "
          + "  c_d_sprotype.name proc_name,                                                                                                                                            "
          + "  (select text_data from c_spec_text where specimen_id = p_special_proc.specimen_id and link_inst = p_special_proc.sp_inst and texttype_id = '$procint') as procint_text, "
          + "  (select text_data from c_spec_text where specimen_id = p_special_proc.specimen_id and link_inst = p_special_proc.sp_inst and texttype_id = '$procres') as procres_text  "
          + "from                                                                                                                                                                      "
          + "  p_special_proc                                                                                                                                                          "
          + "  join c_d_sprotype on (c_d_sprotype.id = p_special_proc.sprotype_id)                                                                                                     "
          + "where                                                                                                                                                                     "
          + "  p_special_proc.specimen_id = ?                                                                                                                                          "
          + "order by                                                                                                                                                                  "
          + "  p_special_proc.sp_inst                                                                                                                                                  "
        );
        pstmt3 = conn.prepareStatement(
            "select                                                                                                   "
          + "  cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                         "
          + "  dsc.abbr as item,                                                                                      "
          + "  dsc.name as item_name,                                                                                 "
          + "  dsv.abbr as val,                                                                                       "
          + "  dsv.name as val_name                                                                                   "
          + "from                                                                                                     "
          + "  c_d_synoptic_ws cdsw                                                                                   "
          + "  join c_d_synoptic_ws_ln cdswl on(cdswl.id = cdsw.id)                                                   "
          + "  join c_d_synoptic_value dsv on(dsv.id = cdswl.value_id)                                                "
          + "  join c_d_synoptic_category dsc on(dsc.id = dsv.category_id)                                            "
          + "where                                                                                                    "
          + "  dsc.name like 'Probe Set (_)'                                                                          "
          + "  and cdsw.id = 'temy85'                                                                                 "
          + "union all                                                                                                "
          + "select                                                                                                   "
          + "  7 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                     "
          + "  dsc.abbr as item,                                                                                      "
          + "  dsc.name as item_name,                                                                                 "
          + "  dsv.abbr as val,                                                                                       "
          + "  dsv.name as val_name                                                                                   "
          + "from                                                                                                     "
          + "  c_d_synoptic_ws cdsw                                                                                   "
          + "  join c_d_synoptic_ws_ln cdswl on(cdswl.id = cdsw.id)                                                   "
          + "  join c_d_synoptic_value dsv on(dsv.id = cdswl.value_id)                                                "
          + "  join c_d_synoptic_category dsc on(dsc.id = dsv.category_id)                                            "
          + "where                                                                                                    "
          + "  dsc.name like 'Probe Set (_)'                                                                          "
          + "  and cdsw.id = 'temy17'                                                                                 "
          + "union all                                                                                                "
          + "select                                                                                                   "
          + "  9 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                     "
          + "  dsc.abbr as item,                                                                                      "
          + "  dsc.name as item_name,                                                                                 "
          + "  dsv.abbr as val,                                                                                       "
          + "  dsv.name as val_name                                                                                   "
          + "from                                                                                                     "
          + "  c_d_synoptic_ws cdsw                                                                                   "
          + "  join c_d_synoptic_ws_ln cdswl on(cdswl.id = cdsw.id)                                                   "
          + "  join c_d_synoptic_value dsv on(dsv.id = cdswl.value_id)                                                "
          + "  join c_d_synoptic_category dsc on(dsc.id = dsv.category_id)                                            "
          + "where                                                                                                    "
          + "  dsc.name like 'Probe Set (_)'                                                                          "
          + "  and cdsw.id = 'temy96'                                                                                 "
          + "union all                                                                                                "
          + "select                                                                                                   "
          + "  14 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                     "
          + "  dsc.abbr as item,                                                                                      "
          + "  dsc.name as item_name,                                                                                 "
          + "  dsv.abbr as val,                                                                                       "
          + "  dsv.name as val_name                                                                                   "
          + "from                                                                                                     "
          + "  c_d_synoptic_ws cdsw                                                                                   "
          + "  join c_d_synoptic_ws_ln cdswl on(cdswl.id = cdsw.id)                                                   "
          + "  join c_d_synoptic_value dsv on(dsv.id = cdswl.value_id)                                                "
          + "  join c_d_synoptic_category dsc on(dsc.id = dsv.category_id)                                            "
          + "where                                                                                                    "
          + "  dsc.name like 'Probe Set (_)'                                                                          "
          + "  and cdsw.id = 'emy48'                                                                                  "
          + "order by                                                                                                 "
          + "  1                                                                                                      "
        );
        pstmt4 = conn.prepareStatement(
            "select                                                                                                   "
          + "  cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                         "
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
          + "      dsv.abbr like 'CF[_]Misc[_]RR_Pos%'                                                                "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]RR_Neg%'                                                                "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Neg%'                                                             "
          + "    )                                                                                                    "
          + "  )                                                                                                      "
          + "  and ssw.worksheet_id = 'temy85'                                                                        "
          + "  and ssw.specimen_id = ?                                                                                "
          + "  and cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) = ?                              "
          + "union all                                                                                                "
          + "select                                                                                                   "
          + "  7 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                     "
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
          + "      dsv.abbr like 'CF[_]Misc[_]RR_Pos%'                                                                "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]RR_Neg%'                                                                "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Neg%'                                                             "
          + "    )                                                                                                    "
          + "  )                                                                                                      "
          + "  and ssw.worksheet_id = 'temy17'                                                                        "
          + "  and ssw.specimen_id = ?                                                                                "
          + "  and 7 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) = ?                          "
          + "union all                                                                                                "
          + "select                                                                                                   "
          + "  9 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                     "
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
          + "      dsv.abbr like 'CF[_]Misc[_]RR_Pos%'                                                                "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]RR_Neg%'                                                                "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Neg%'                                                             "
          + "    )                                                                                                    "
          + "  )                                                                                                      "
          + "  and ssw.worksheet_id = 'temy96'                                                                        "
          + "  and ssw.specimen_id = ?                                                                                "
          + "  and 9 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) = ?                          "
          + "union all                                                                                                "
          + "select                                                                                                   "
          + "  14 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) as probe_no,                     "
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
          + "      dsv.abbr like 'CF[_]Misc[_]RR_Pos%'                                                                "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Pos%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]RR_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]CN_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]AR_Neg%'                                                             "
          + "      or dsv.abbr like 'CF[_]Misc[_]OR_Neg%'                                                             "
          + "    )                                                                                                    "
          + "  )                                                                                                      "
          + "  and ssw.worksheet_id = 'emy48'                                                                         "
          + "  and ssw.specimen_id = ?                                                                                "
          + "  and 14 + cast(substring(dsc.name, charindex('(', dsc.name) + 1, 1) as int) = ?                          "
          + "order by                                                                                                 "
          + "  1                                                                                                      "
        );
    }


    public CoPathCase getCoPathXXX(String accNo) throws SQLException {
        CoPathCase coPathCase = null;
        pstmt1.setString(1, accNo);
        ResultSet rs1 = pstmt1.executeQuery();
        if(rs1.next()) {
            coPathCase = new CoPathCase(rs1);
        }
        return coPathCase;
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
            ResultSet rs3 = pstmt3.executeQuery();
            while(rs3.next()) {
                FishProbe fishProbe = new CoPathCase.FishProbe(rs3);
                coPathCase.fishProbes.add(fishProbe);
                pstmt4.setString(1, coPathCase.specimenId);
                pstmt4.setInt(2, fishProbe.probeNumber);
                pstmt4.setString(3, coPathCase.specimenId);
                pstmt4.setInt(4, fishProbe.probeNumber);
                pstmt4.setString(5, coPathCase.specimenId);
                pstmt4.setInt(6, fishProbe.probeNumber);
                pstmt4.setString(7, coPathCase.specimenId);
                pstmt4.setInt(8, fishProbe.probeNumber);
                ResultSet rs4 = pstmt4.executeQuery();
                while(rs4.next()) {
                    fishProbe.setVariationProperties(rs4);
                }
                rs4.close();
            }
            rs3.close();
            coPathCase.pathNetResults = pathNetResultFinder.getPathNetResultsByEmpiProximateToCollectionDate(coPathCase.empi, coPathCase.collectionDate);
            //if(coPathCase.getPathNetResultMap().get("SPEINTERP") != null && coPathCase.getPathNetResultMap().get("SPEINTERP").accNo != null) {
            //    coPathCase.sebiaCaseSerum = sebiaCaseFinder.getSebiaCaseByAccNo(coPathCase.getPathNetResultMap().get("SPEINTERP").accNo);
            //    coPathCase.sebiaCaseSerumGelControl = sebiaCaseFinder.getSebiaGelControlByAccNo(coPathCase.getPathNetResultMap().get("SPEINTERP").accNo);
            //}
            //if(coPathCase.getPathNetResultMap().get("Urine Protein Electrophoresis") != null && coPathCase.getPathNetResultMap().get("Urine Protein Electrophoresis").accNo != null) {
            //    coPathCase.sebiaCaseUrine = sebiaCaseFinder.getSebiaCaseByAccNo(coPathCase.getPathNetResultMap().get("Urine Protein Electrophoresis").accNo);
            //    coPathCase.sebiaCaseUrineGelControl = sebiaCaseFinder.getSebiaGelControlByAccNo(coPathCase.getPathNetResultMap().get("Urine Protein Electrophoresis").accNo);
            //}
        }
        rs1.close();
        return coPathCase;
    }

    public List<CoPathCase> getCoPathCasesByFacilityMrnAndDob(String facilityMrn, Date dob, PathNetResultFinder pathNetResultFinder, SebiaCaseFinder sebiaCaseFinder) throws SQLException {
        List<CoPathCase> coPathCases = new ArrayList<>();
        pstmt0.setString(1, facilityMrn);
        pstmt0.setDate(2, dob);
        ResultSet rs = pstmt0.executeQuery();
        while(rs.next()) {
            System.out.println(facilityMrn + ": " + rs.getString(1));
            coPathCases.add(getCoPathCaseByAccNo(rs.getString(1), pathNetResultFinder, sebiaCaseFinder));
        }
        return coPathCases;
    }
    
    public List<CoPathCase> getCoPathCasesByFacilityMrn(String facilityMrn, PathNetResultFinder pathNetResultFinder, SebiaCaseFinder sebiaCaseFinder) throws SQLException {
        List<CoPathCase> coPathCases = new ArrayList<>();
        pstmt0_noDob.setString(1, facilityMrn);
        ResultSet rs = pstmt0_noDob.executeQuery();
        while(rs.next()) {
            System.out.println(facilityMrn + ": " + rs.getString(1));
            coPathCases.add(getCoPathCaseByAccNo(rs.getString(1), pathNetResultFinder, sebiaCaseFinder));
        }
        return coPathCases;
    }

}
