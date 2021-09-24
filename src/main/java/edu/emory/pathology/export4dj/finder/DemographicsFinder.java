package edu.emory.pathology.export4dj.finder;

import edu.emory.pathology.export4dj.data.Demographics;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Geoffrey H. Smith
 */
public class DemographicsFinder {

    private final Connection connCdw;
    private final Connection connCoPath;
    
    private final PreparedStatement pstmt1;
    private final PreparedStatement pstmt2;
    
    public DemographicsFinder(Connection connCdw, Connection connCoPath) throws SQLException {
        this.connCdw = connCdw;
        this.connCoPath = connCoPath;
        pstmt1 = connCoPath.prepareStatement(
            "select                                                                           "
          + "  r_pat_demograph.lastname as lastname                                           "
          + "from                                                                             "
          + "  c_specimen                                                                     "
          + "  join r_pat_demograph on (r_pat_demograph.patdemog_id = c_specimen.patdemog_id) "
          + "where                                                                            "
          + "  c_specimen.specnum_formatted = ?                                               "
        );
        pstmt2 = connCdw.prepareStatement(
            "select                                                    "
          + "  patient_birth_dt birth_dt,                              "
          + "  patient_death_dt death_dt,                              "
          + "  ethnicity_cd ethnicity,                                 "
          + "  race_cd race,                                           "
          + "  ethnic_group_cd ethnic_group,                           "
          + "  gender_cd gender,                                       "
          + "  patient_address_zip_desc zip_code,                       "
          + "  (select max(discharge_dt) from ehcvw.fact_encounter where fact_encounter.patient_key = lkp_patient.patient_key) latest_discharge_dt "
          + "from                                                      "
          + "  ehcvw.lkp_patient                                       "
          + "where                                                     "
          + "  empi_nbr = ?                                            "
          + "  and patient_last_nm = ?                                 "
          + "  and current_record_ind = 1                              "
        );
    }
    
    public Demographics getDemographicsByEmpiAndAccNo(String empi, String accNo) throws SQLException {
        Demographics demographics = null;
        pstmt1.setString(1, accNo);
        ResultSet rs1 = pstmt1.executeQuery();
        if(rs1.next()) {
            pstmt2.setString(1, empi);
            pstmt2.setString(2, rs1.getString("lastname"));
            ResultSet rs2 = pstmt2.executeQuery();
            if(rs2.next()) {
                demographics = new Demographics(rs2);
            }
            rs2.close();
        }
        rs1.close();
        return demographics;
    }
    
}
