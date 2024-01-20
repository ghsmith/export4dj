package edu.emory.pathology.export4dj.finder;

import edu.emory.pathology.export4dj.data.PathNetResult;
import edu.emory.pathology.export4dj.data.Vital;
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
public class VitalFinder {
    
    private final Connection conn;
    private final PreparedStatement pstmt1;
    private final PreparedStatement pstmt2;

    public VitalFinder(Connection conn) throws SQLException {
        this.conn = conn;
        pstmt1 = conn.prepareStatement(
"    select\n" +
"      lsrt.structured_result_type_key,\n" +
"      max (fsn.structured_note_key) keep\n" +
"      (\n" +
"        dense_rank first order by\n" +
"          decode\n" +
"            (\n" +
"              sign(trunc(fsn.recorded_dt) - ?),\n" +
"               0, 0,\n" +
"              -1, 1,\n" +
"              +1, 2\n" +
"            ) asc,\n" +
"          abs(trunc(fsn.recorded_dt) - ?) asc\n" +
"      ) structured_note_key\n" +
"    from\n" +
"      ehcvw.fact_structured_note fsn\n" +
"      join\n" +
"      (\n" +
"        select\n" +
"          structured_result_type_key\n" +
"        from\n" +
"          ehcvw.lkp_structured_result_type\n" +
"        where\n" +
"          structured_result_type_key in\n" +
"          (\n" +
" 5571\n" +
",5578\n" +
",5557\n" +
",5568\n" +
",5541\n" +
",5584\n" +
",5581\n" +
",35851\n" +
"          )\n" +
"      ) lsrt on (lsrt.structured_result_type_key = fsn.structured_result_type_key)\n" +
"    where\n" +
"      fsn.patient_key in (select patient_key from ehcvw.lkp_patient where empi_nbr = ?)\n" +
"      and fsn.recorded_dt > ? - 45\n" +
"      and fsn.recorded_dt < ? + 45\n" +
"    group by\n" +
"      lsrt.structured_result_type_key"                
        );
        pstmt2 = conn.prepareStatement(
"select\n" +
"  lsrt.structured_result_type_desc result_name,\n" +
"  fsn.result_tval result_value,\n" +
"  lum.unit_measure_desc result_uom,\n" +
"  trunc(fsn.recorded_dt) - ? recorded_days_delta\n" +
"from\n" +
"  ehcvw.lkp_structured_result_type lsrt\n" +
"  left outer join ehcvw.fact_structured_note fsn on(fsn.structured_result_type_key = lsrt.structured_result_type_key and fsn.structured_note_key in(?, ?, ?, ?, ?, ?, ?, ?))\n" +
"  left outer join ehcvw.lkp_unit_measure lum on (lum.unit_measure_key = fsn.unit_measure_key)\n" +
"where\n" +
"  lsrt.structured_result_type_key in\n" +
"  (\n" +
" 5571\n" +
",5578\n" +
",5557\n" +
",5568\n" +
",5541\n" +
",5584\n" +
",5581\n" +
",35851\n" +
"  )\n" +
"order by\n" +
"  decode\n" +
"  (\n" +
"    lsrt.structured_result_type_desc\n" +
",'Height in Inches'                 ,  1\n" +
",'Calculated Height in Inches'      ,  2\n" +
",'Height in cm'                     ,  3\n" +
",'Calculated Height in cm'          ,  4\n" +
",'Weight in Pounds'                 ,  5\n" +
",'Weight in kg'                     ,  6\n" +
",'Calculated Weight in Kg'          ,  7\n" +
",'Body Mass Index'                  ,  8\n" +
"  )"
        );

    }

    public List<Vital> getVitalsByEmpiProximateToCollectionDate(String empi, Date collectionDate) {
        List<Vital> vitals = new ArrayList<>();
        try {
            pstmt1.setDate(1, collectionDate);
            pstmt1.setDate(2, collectionDate);
            pstmt1.setString(3, empi);
            pstmt1.setDate(4, collectionDate);
            pstmt1.setDate(5, collectionDate);
            ResultSet rs1 = pstmt1.executeQuery();
            pstmt2.setDate(1, collectionDate);
            pstmt2.setLong(2, 0);
            pstmt2.setLong(3, 0);
            pstmt2.setLong(4, 0);
            pstmt2.setLong(5, 0);
            pstmt2.setLong(6, 0);
            pstmt2.setLong(7, 0);
            pstmt2.setLong(8, 0);
            pstmt2.setLong(9, 0);
            if(rs1.next()) { pstmt2.setLong(2, rs1.getLong("structured_note_key")); }
            if(rs1.next()) { pstmt2.setLong(3, rs1.getLong("structured_note_key")); }
            if(rs1.next()) { pstmt2.setLong(4, rs1.getLong("structured_note_key")); }
            if(rs1.next()) { pstmt2.setLong(5, rs1.getLong("structured_note_key")); }
            if(rs1.next()) { pstmt2.setLong(6, rs1.getLong("structured_note_key")); }
            if(rs1.next()) { pstmt2.setLong(7, rs1.getLong("structured_note_key")); }
            if(rs1.next()) { pstmt2.setLong(8, rs1.getLong("structured_note_key")); }
            if(rs1.next()) { pstmt2.setLong(9, rs1.getLong("structured_note_key")); }
            ResultSet rs2 = pstmt2.executeQuery();
            while(rs2.next()) {
                vitals.add(new Vital(rs2));
            }
            rs1.close();
            rs2.close();
        }
        catch(SQLException e) {
            System.out.println(String.format("error getting resutls for EMPI %s and collection date %s", empi, collectionDate));
            e.printStackTrace();
        }
        return vitals;
    }
    
}
