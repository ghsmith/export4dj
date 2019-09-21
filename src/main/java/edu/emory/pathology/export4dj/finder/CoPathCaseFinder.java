package edu.emory.pathology.export4dj.finder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import edu.emory.pathology.export4dj.data.CoPathCase;

/**
 *
 * @author Geoffrey H. Smith
 */
public class CoPathCaseFinder {
    
    private Connection conn;
    private PreparedStatement pstmt1;

    public CoPathCaseFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt1 = conn.prepareStatement(
            "select top 1                                                                       "
          + "    c_specimen.specimen_id,                                                        "
          + "    c_specimen.specnum_formatted,                                                  "
          + "    c_specimen.accession_date,                                                     "
          + "    p_part.datetime_taken,                                                         "
          + "    r_pat_demograph.universal_mednum_stripped                                      "
          + "from                                                                               "
          + "    c_specimen                                                                     "
          + "    join r_pat_demograph on (r_pat_demograph.patdemog_id = c_specimen.patdemog_id) "
          + "    join p_part on (p_part.specimen_id = c_specimen.specimen_id)                   "
          + "where                                                                              "
          + "    c_specimen.specnum_formatted = ?                                               "
        );
    }
    
    public CoPathCase getCoPathCaseByAccNo(String accNo) throws SQLException {
        CoPathCase coPathCase = null;
        pstmt1.setString(1, accNo);
        ResultSet rs = pstmt1.executeQuery();
        if(rs.next()) {
            coPathCase = new CoPathCase(rs);
        }
        rs.close();
        pstmt1.close();
        return coPathCase;
    }
    
}
