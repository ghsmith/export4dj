package edu.emory.pathology.export4dj.data;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Geoffrey H. Smith
 */
public class PathNetResult {

    public String resultName;
    public Integer collectionDateDelta;
    public String value;
    public String uom;
    public String flag;
    public String interp;

    public PathNetResult(ResultSet rs) throws SQLException {
        this.resultName = rs.getString("result_name");
        this.collectionDateDelta = rs.getInt("collection_days_delta"); if(rs.wasNull()) { this.collectionDateDelta = null; }
        this.value = rs.getString("result_value");
        this.uom = rs.getString("result_uom");
        this.flag = rs.getString("result_flag");
        if(this.resultName.matches(".*(?i:interp).*")) {
            this.interp = rs.getString("result_narrative");
        }
    }
    
}
