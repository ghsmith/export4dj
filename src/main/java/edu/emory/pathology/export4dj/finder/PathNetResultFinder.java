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
    
    private final Connection conn;
    private final PreparedStatement pstmt1;

    public PathNetResultFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt1 = conn.prepareStatement(
            "with proximate_lab_result_keys as                                                                                                                                      "
          + "(                                                                                                                                                                      "
          + "    select                                                                                                                                                             "
          + "      lsrt.structured_result_type_key,                                                                                                                                 "
          + "      max (frl.result_lab_key) keep                                                                                                                                    "
          + "      (                                                                                                                                                                "
          + "        dense_rank first order by                                                                                                                                      "
          + "          decode                                                                                                                                                       "
          + "            (                                                                                                                                                          "
          + "              sign(trunc(frl.specimen_collect_dt) - ?),                                                                                                                "
          + "               0, 0,                                                                                                                                                   "
          + "              -1, 1,                                                                                                                                                   "
          + "              +1, 2                                                                                                                                                    "
          + "            ) asc,                                                                                                                                                     "
          + "          abs(trunc(frl.specimen_collect_dt) - ?) asc                                                                                                                  "
          + "      ) result_lab_key                                                                                                                                                 "
          + "    from                                                                                                                                                               "
          + "      ehcvw.fact_result_lab frl                                                                                                                                        "
          + "      join                                                                                                                                                             "
          + "      (                                                                                                                                                                "
          + "        select                                                                                                                                                         "
          + "          structured_result_type_key                                                                                                                                   "
          + "        from                                                                                                                                                           "
          + "          ehcvw.lkp_structured_result_type                                                                                                                             "
          + "        where                                                                                                                                                          "
          + "          structured_result_type_id in                                                                                                                                 "
          + "          (                                                                                                                                                            "

          + " '1512214'   " // White Blood Count
          + ",'1512995'   " // Red Blood Cell Count
          + ",'1513817'   " // Hemoglobin
          + ",'1513889'   " // Hematocrit
          + ",'1513468'   " // MCV
          + ",'1513472'   " // MCH
          + ",'1513471'   " // MCHC
          + ",'651859719' " // Red Cell Distribution Width-CV
          + ",'651859723' " // Red Cell Distribution Width-SD
          + ",'1512939'   " // RDW
          + ",'200186179' " // Auto Nucleated Red Cell Count
          + ",'1513222'   " // Platelet Count       
                    
          + ",'1515017'   " // Albumin Level
          + ",'1514422'   " // Creatinine
          + ",'1514676'   " // Calcium Level Total
          + ",'1513585'   " // Lactate Dehydrogenase
          + ",'1514852'   " // Beta 2 Microglobulin Level

          + ",'1513691'   " // Immunoglobulin IgG Level Total
          + ",'1513695'   " // Immunoglobulin IgA Level
          + ",'1513674'   " // Immunoglobulin IgM Level Total
          + ",'1514077'   " // Free Kappa
          + ",'1514074'   " // Free Lambda

          + ",'1512541'   " // Total Protein
          + ",'77877710'  " // Albumin Fraction
          + ",'1515120'   " // Alpha-1 Fraction
          + ",'1515116'   " // Alpha-2 Fraction
          + ",'1514857'   " // Beta-1 Fraction
          + ",'1514856'   " // Beta-2 Fraction
          + ",'1514009'   " // Gamma Globulin Fraction
          + ",'1513299'   " // Paraprotein Concentration
          + ",'1512723'   " // SPEINTERP
          + ",'1513696'   " // Immunofixation Interpretation

          + ",'1512289'   " // Total Protein Urine 24 Hour
          + ",'618860967' " // Paraprotein/24 hours (URINE)
          + ",'1512366'   " // Urine Protein Electrophoresis
          + ",'844756952' " // Urine Immunofixation Interp
          + ",'1513700'   " // Urine Immunoelectrophoresis Interp (NO LONGER USED)

          + ",'1514076'   " // Free Kappa/Lambda Ratio
                    
          + "          )                                                                                                                                                            "
          + "      ) lsrt on (lsrt.structured_result_type_key = frl.structured_result_type_key)                                                                                     "
          + "    where                                                                                                                                                              "
          + "      frl.patient_key in (select patient_key from ehcvw.lkp_patient where empi_nbr = ?)                                                                                "
          + "      and frl.specimen_collect_dt > ? - 45                                                                                                                             "
          + "      and frl.specimen_collect_dt < ? + 45                                                                                                                             "
          + "    group by                                                                                                                                                           "
          + "      lsrt.structured_result_type_key                                                                                                                                  "
          + ")                                                                                                                                                                      "
          + "select                                                                                                                                                                 "
          + "  frl.accession_nbr accno,                                                                                                                                             "
          + "  lsrt.structured_result_type_desc result_name,                                                                                                                        "
          + "  frl.result_lab_tval result_value,                                                                                                                                    "
          + "  lum.unit_measure_desc result_uom,                                                                                                                                    "
          + "  lri.result_interpretation_desc result_flag,                                                                                                                          "
          + "  trunc(frl.specimen_collect_dt) - ? collection_days_delta,                                                                                                            "
          + "  (select listagg(event_document_abstract_txt, ';' on overflow truncate) within group (order by order_key) from ehcvw.fact_event_document where order_key = frl.order_key) result_narrative "
          + "from                                                                                                                                                                   "
          + "  (                                                                                                                                                                    "
          + "    select                                                                                                                                                             "
          + "      structured_result_type_key,                                                                                                                                      "
          + "      structured_result_type_desc                                                                                                                                      "
          + "    from                                                                                                                                                               "
          + "      ehcvw.lkp_structured_result_type                                                                                                                                 "
          + "    where                                                                                                                                                              "
          + "      structured_result_type_id in                                                                                                                                     "
          + "      (                                                                                                                                                                "

          + " '1512214'   " // White Blood Count
          + ",'1512995'   " // Red Blood Cell Count
          + ",'1513817'   " // Hemoglobin
          + ",'1513889'   " // Hematocrit
          + ",'1513468'   " // MCV
          + ",'1513472'   " // MCH
          + ",'1513471'   " // MCHC
          + ",'651859719' " // Red Cell Distribution Width-CV
          + ",'651859723' " // Red Cell Distribution Width-SD
          + ",'1512939'   " // RDW
          + ",'200186179' " // Auto Nucleated Red Cell Count
          + ",'1513222'   " // Platelet Count       
                    
          + ",'1515017'   " // Albumin Level
          + ",'1514422'   " // Creatinine
          + ",'1514676'   " // Calcium Level Total
          + ",'1513585'   " // Lactate Dehydrogenase
          + ",'1514852'   " // Beta 2 Microglobulin Level

          + ",'1513691'   " // Immunoglobulin IgG Level Total
          + ",'1513695'   " // Immunoglobulin IgA Level
          + ",'1513674'   " // Immunoglobulin IgM Level Total
          + ",'1514077'   " // Free Kappa
          + ",'1514074'   " // Free Lambda

          + ",'1512541'   " // Total Protein
          + ",'77877710'  " // Albumin Fraction
          + ",'1515120'   " // Alpha-1 Fraction
          + ",'1515116'   " // Alpha-2 Fraction
          + ",'1514857'   " // Beta-1 Fraction
          + ",'1514856'   " // Beta-2 Fraction
          + ",'1514009'   " // Gamma Globulin Fraction
          + ",'1513299'   " // Paraprotein Concentration
          + ",'1512723'   " // SPEINTERP
          + ",'1513696'   " // Immunofixation Interpretation

          + ",'1512289'   " // Total Protein Urine 24 Hour
          + ",'618860967' " // Paraprotein/24 hours (URINE)
          + ",'1512366'   " // Urine Protein Electrophoresis
          + ",'844756952' " // Urine Immunofixation Interp
          + ",'1513700'   " // Urine Immunoelectrophoresis Interp (NO LONGER USED)

          + ",'1514076'   " // Free Kappa/Lambda Ratio
                    
          + "      )                                                                                                                                                                "
          + "  ) lsrt                                                                                                                                                               "
          + "  left outer join proximate_lab_result_keys plrk on (plrk.structured_result_type_key = lsrt.structured_result_type_key)                                                "
          + "  left outer join ehcvw.fact_result_lab frl on (frl.result_lab_key = plrk.result_lab_key)                                                                              "
          + "  left outer join ehcvw.lkp_unit_measure lum on (lum.unit_measure_key = frl.unit_measure_key)                                                                          "
          + "  left outer join ehcvw.lkp_result_interpretation lri on (lri.result_interpretation_key = frl.result_interpretation_key)                                               "
          + "order by                                                                                                                                                               "
          + "  decode                                                                                                                                                               "
          + "  (                                                                                                                                                                    "
          + "    lsrt.structured_result_type_desc                                                                                                                                   "

          + ",'White Blood Count'                 ,  1 "
          + ",'Red Blood Cell Count'              ,  2 "
          + ",'Hemoglobin'                        ,  3 "
          + ",'Hematocrit'                        ,  4 "
          + ",'MCV'                               ,  5 "
          + ",'MCH'                               ,  6 "
          + ",'MCHC'                              ,  7 "
          + ",'Red Cell Distribution Width-CV'    ,  8 "
          + ",'Red Cell Distribution Width-SD'    ,  9 "
          + ",'RDW'                               , 10 "
          + ",'Auto Nucleated Red Cell Count'     , 11 "
          + ",'Platelet Count'                    , 12 "
          + ",'Albumin Level'                     , 13 "
          + ",'Creatinine'                        , 14 "
          + ",'Calcium Level Total'               , 15 "
          + ",'Lactate Dehydrogenase'             , 16 "
          + ",'Beta 2 Microglobulin Level'        , 17 "
          + ",'Immunoglobulin IgA Level'          , 18 "
          + ",'Immunoglobulin IgG Level Total'    , 19 "
          + ",'Immunoglobulin IgM Level Total'    , 20 "
          + ",'Free Kappa'                        , 21 "
          + ",'Free Lambda'                       , 22 "
          + ",'Total Protein'                     , 23 "
          + ",'Albumin Fraction'                  , 24 "
          + ",'Alpha-1 Fraction'                  , 25 "
          + ",'Alpha-2 Fraction'                  , 26 "
          + ",'Beta-1 Fraction'                   , 27 "
          + ",'Beta-2 Fraction'                   , 28 "
          + ",'Gamma Globulin Fraction'           , 29 "
          + ",'Paraprotein Concentration'         , 30 "
          + ",'SPEINTERP'                         , 31 "
          + ",'Immunfixation Interpretation'      , 32 "
          + ",'Total Protein Urine 24 Hour'       , 33 "
          + ",'Paraprotein/24 hours'              , 34 "
          + ",'Urine Protein Electrophoresis'     , 35 "
          + ",'Urine Immunofixation Interp'       , 36 "
          + ",'Urine Immunoelectrophoresis Interp', 37 "
          + ",'Free Kappa/Lambda Ratio'           , 38 "

          + "  )                                                                                                                                                                    "
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
