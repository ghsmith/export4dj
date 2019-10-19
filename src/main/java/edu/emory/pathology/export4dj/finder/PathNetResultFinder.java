package edu.emory.pathology.export4dj.finder;

import edu.emory.pathology.export4dj.data.PathNetResult;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Geoffrey H. Smith
 */
public class PathNetResultFinder {
    
    private Connection conn;
    private PreparedStatement pstmt1;

    public PathNetResultFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt1 = conn.prepareStatement(
            "with proximate_lab_result_keys as                                                                                                                                                                                "
          + "(                                                                                                                                                                                                                "
          + "    select                                                                                                                                                                                                       "
          + "      lsrt.structured_result_type_key,                                                                                                                                                                           "
          + "      max (frl.result_lab_key) keep                                                                                                                                                                              "
          + "      (                                                                                                                                                                                                          "
          + "        dense_rank first order by                                                                                                                                                                                "
          + "          decode                                                                                                                                                                                                 "
          + "            (                                                                                                                                                                                                    "
          + "              sign(trunc(frl.specimen_collect_dt) - ?),                                                                                                                                                          "
          + "               0, 0,                                                                                                                                                                                             "
          + "              -1, 1,                                                                                                                                                                                             "
          + "              +1, 2                                                                                                                                                                                              "
          + "            ) asc,                                                                                                                                                                                               "
          + "          abs(trunc(frl.specimen_collect_dt) - ?) asc                                                                                                                                                            "
          + "      ) result_lab_key                                                                                                                                                                                           "
          + "    from                                                                                                                                                                                                         "
          + "      ehcvw.fact_result_lab frl                                                                                                                                                                                  "
          + "      join                                                                                                                                                                                                       "
          + "      (                                                                                                                                                                                                          "
          + "        select                                                                                                                                                                                                   "
          + "          structured_result_type_key                                                                                                                                                                             "
          + "        from                                                                                                                                                                                                     "
          + "          ehcvw.lkp_structured_result_type                                                                                                                                                                       "
          + "        where                                                                                                                                                                                                    "
          + "          structured_result_type_id in ('1513299','618860967','1514077','1514074','1513691','1513695','1513674','1513585','1514852','1515017','1514422','1514676', '1512723', '1513696', '1513700', '844756952', '1512366') "
          + "      ) lsrt on (lsrt.structured_result_type_key = frl.structured_result_type_key)                                                                                                                               "
          + "    where                                                                                                                                                                                                        "
          + "      frl.patient_key in (select patient_key from ehcvw.lkp_patient where empi_nbr = ?)                                                                                                                          "
          + "      and frl.specimen_collect_dt > ? - 45                                                                                                                                                                       "
          + "      and frl.specimen_collect_dt < ? + 45                                                                                                                                                                       "
          + "    group by                                                                                                                                                                                                     "
          + "      lsrt.structured_result_type_key                                                                                                                                                                            "
          + ")                                                                                                                                                                                                                "
          + "select                                                                                                                                                                                                           "
          + "  lsrt.structured_result_type_desc result_name,                                                                                                                                                                  "
          + "  frl.result_lab_tval result_value,                                                                                                                                                                              "
          + "  lum.unit_measure_desc result_uom,                                                                                                                                                                              "
          + "  lri.result_interpretation_desc result_flag,                                                                                                                                                                    "
          + "  trunc(frl.specimen_collect_dt) - ? collection_days_delta,                                                                                                                                                      "
          + "  (select listagg(event_document_abstract_txt, ';') within group (order by order_key) from ehcvw.fact_event_document where order_key = frl.order_key) result_narrative                                           "
          + "from                                                                                                                                                                                                             "
          + "  (                                                                                                                                                                                                              "
          + "    select                                                                                                                                                                                                       "
          + "      structured_result_type_key,                                                                                                                                                                                "
          + "      structured_result_type_desc                                                                                                                                                                                "
          + "    from                                                                                                                                                                                                         "
          + "      ehcvw.lkp_structured_result_type                                                                                                                                                                           "
          + "    where                                                                                                                                                                                                        "
          + "      structured_result_type_id in ('1513299','618860967','1514077','1514074','1513691','1513695','1513674','1513585','1514852','1515017','1514422','1514676', '1512723', '1513696', '1513700', '844756952', '1512366') "
          + "  ) lsrt                                                                                                                                                                                                         "
          + "  left outer join proximate_lab_result_keys plrk on (plrk.structured_result_type_key = lsrt.structured_result_type_key)                                                                                          "
          + "  left outer join ehcvw.fact_result_lab frl on (frl.result_lab_key = plrk.result_lab_key)                                                                                                                        "
          + "  left outer join ehcvw.lkp_unit_measure lum on (lum.unit_measure_key = frl.unit_measure_key)                                                                                                                    "
          + "  left outer join ehcvw.lkp_result_interpretation lri on (lri.result_interpretation_key = frl.result_interpretation_key)                                                                                         "
          + "order by                                                                                                                                                                                                         "
          + "  decode                                                                                                                                                                                                         "
          + "  (                                                                                                                                                                                                              "
          + "    lsrt.structured_result_type_desc,                                                                                                                                                                            "
          + "    'Albumin Level', 1,                                                                                                                                                                                          "
          + "    'Beta 2 Microglobulin Level', 2,                                                                                                                                                                             "
          + "    'Calcium Level Total', 3,                                                                                                                                                                                    "
          + "    'Creatinine', 4,                                                                                                                                                                                             "
          + "    'Free Kappa', 5,                                                                                                                                                                                             "
          + "    'Free Lambda', 6,                                                                                                                                                                                            "
          + "    'Immunoglobulin IgA Level', 7,                                                                                                                                                                               "
          + "    'Immunoglobulin IgG Level Total', 8,                                                                                                                                                                         "
          + "    'Immunoglobulin IgM Level Total', 9,                                                                                                                                                                         "
          + "    'Lactate Dehydrogenase', 10,                                                                                                                                                                                 "
          + "    'Paraprotein Concentration', 11,                                                                                                                                                                             "
          + "    'SPEINTERP', 12,                                                                                                                                                                                             "
          + "    'Immunfixation Interpretation', 13,                                                                                                                                                                          "
          + "    'Paraprotein/24 hours', 14,                                                                                                                                                                                  "
          + "    'Urine Protein Electrophoresis', 15,                                                                                                                                                                         "
          + "    'Urine Immunofixation Interp', 16,                                                                                                                                                                           "
          + "    'Urine Immunoelectrophoresis Interp', 17                                                                                                                                                                     "
          + "  )                                                                                                                                                                                                              "
        );       
    }

    public List<PathNetResult> getPathNetResultsByEmpiProximateToCollectionDate(String empi, Date collectionDate) {
        List<PathNetResult> pathNetResults = new ArrayList<>();
        try {
            pstmt1.setDate(1, collectionDate);
            pstmt1.setDate(2, collectionDate);
            pstmt1.setString(3, empi);
            pstmt1.setDate(4, collectionDate);
            pstmt1.setDate(5, collectionDate);
            pstmt1.setDate(6, collectionDate);
            ResultSet rs1 = pstmt1.executeQuery();
            while(rs1.next()) {
                pathNetResults.add(new PathNetResult(rs1));
            }
            rs1.close();
        }
        catch(SQLException e) {
            System.out.println(String.format("error getting resutls for EMPI %s and collection date %s", empi, collectionDate));
            e.printStackTrace();
        }
        return pathNetResults;
    }
    
}
