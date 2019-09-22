package edu.emory.pathology.export4dj.finder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import edu.emory.pathology.export4dj.data.CoPathCase;
import edu.emory.pathology.export4dj.data.CoPathCase.CoPathProcedure;
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

    public CoPathCaseFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt1 = conn.prepareStatement(
            "select top 1                                                                                                               "
          + "  c_specimen.specimen_id,                                                                                                  "
          + "  c_specimen.specnum_formatted,                                                                                            "
          + "  c_specimen.accession_date,                                                                                               "
          + "  p_part.datetime_taken,                                                                                                   "
          + "  r_pat_demograph.universal_mednum_stripped,                                                                               "
          + "  (select text_data from c_spec_text where specimen_id = c_specimen.specimen_id and texttype_id = '$final') as final_text, "
          + "  (select text_data from c_spec_text where specimen_id = c_specimen.specimen_id and texttype_id = 'emy41') as karyotype    "
          + "from                                                                                                                       "
          + "  c_specimen                                                                                                               "
          + "  join r_pat_demograph on (r_pat_demograph.patdemog_id = c_specimen.patdemog_id)                                           "
          + "  join p_part on (p_part.specimen_id = c_specimen.specimen_id)                                                             "
          + "where                                                                                                                      "
          + "  c_specimen.specnum_formatted = ?                                                                                         "
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
    }
    
    public CoPathCase getCoPathCaseByAccNo(String accNo, PathNetResultFinder pathNetResultFinder) throws SQLException {
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
            coPathCase.pathNetResults = pathNetResultFinder.getPathNetResultsByEmpiProximateToCollectionDate(coPathCase.empi, coPathCase.collectionDate);
            coPathCase.procedureMap = new HashMap<>();
            for(CoPathProcedure coPathProcedure : coPathCase.procedures) {
                coPathCase.procedureMap.put(coPathProcedure.procName, coPathProcedure);
            }
            coPathCase.pathNetResultMap = new HashMap<>();
            for(PathNetResult pathNetResult : coPathCase.pathNetResults) {
                coPathCase.pathNetResultMap.put(pathNetResult.resultName, pathNetResult);
            }
        }
        rs1.close();
        return coPathCase;
    }
    
}
