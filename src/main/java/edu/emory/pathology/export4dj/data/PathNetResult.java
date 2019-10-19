package edu.emory.pathology.export4dj.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author Geoffrey H. Smith
 */
@XmlRootElement
public class PathNetResult {

    @XmlAttribute
    public String resultName;
    @XmlAttribute
    public Integer collectionDateDelta;
    @XmlAttribute
    public String value;
    @XmlAttribute
    public String uom;
    @XmlAttribute
    public String flag;
    public String interp;

    public PathNetResult() {
    }

    public PathNetResult(ResultSet rs) throws SQLException {
        this.resultName = rs.getString("result_name");
        this.collectionDateDelta = rs.getInt("collection_days_delta"); if(rs.wasNull()) { this.collectionDateDelta = null; }
        this.value = rs.getString("result_value");
        this.uom = rs.getString("result_uom");
        this.flag = rs.getString("result_flag");
        if(this.resultName.matches(".*(?i:interp).*") || this.resultName.matches("Urine Protein Electrophoresis")) {
            this.interp = rs.getString("result_narrative") == null ? null : rs.getString("result_narrative")
                .replace("\u0008", "") // there are ASCII 08 (backspace?) characters in this column
                .replace("\u00a0", " ") // thar are ASCII A0 (non-breaking space) characaters in this column
                .replace("\u00b7", " ") // thar are ASCII B7 (dot) characaters in this column
                .replace("\r", "")
                .replaceAll("(?m)\\s+$", "")
                .replaceAll("(?m)^", "")
                .replaceAll("^\n", "");
        }
    }
    
}
