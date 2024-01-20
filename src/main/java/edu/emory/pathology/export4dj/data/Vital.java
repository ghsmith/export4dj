package edu.emory.pathology.export4dj.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author Geoffrey H. Smith
 */
@XmlRootElement
public class Vital {

    @XmlAttribute
    public String resultName;
    @XmlAttribute
    public Integer recordedDateDelta;
    @XmlAttribute
    public String value;
    @XmlAttribute
    public String uom;

    public Vital() {
    }

    public Vital(ResultSet rs) throws SQLException {
        this.resultName = rs.getString("result_name");
        this.recordedDateDelta = rs.getInt("recorded_days_delta"); if(rs.wasNull()) { this.recordedDateDelta = null; }
        this.value = rs.getString("result_value");
        this.uom = rs.getString("result_uom");
    }

    @Override
    public String toString() {
        return "Vital{" + "resultName=" + resultName + ", recordedDateDelta=" + recordedDateDelta + ", value=" + value + ", uom=" + uom + '}';
    }
    
}
