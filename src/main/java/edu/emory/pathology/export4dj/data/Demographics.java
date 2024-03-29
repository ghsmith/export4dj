package edu.emory.pathology.export4dj.data;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 *
 * @author Geoffrey H. Smith
 */
@XmlRootElement
public class Demographics {

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
    @XmlTransient
    public Date birthDate;
    @XmlTransient
    public Date deathDate;
    @XmlTransient
    public Date latestDischargeDate;
    @XmlAttribute
    public String ethnicity;
    @XmlAttribute
    public String race;
    @XmlAttribute
    public String ethnicGroup;
    @XmlAttribute
    public String gender;
    @XmlAttribute
    public String zipCode;

    public Demographics() {
    }
    
    public Demographics(ResultSet rs) throws SQLException {
        this.birthDate = rs.getDate("birth_dt");
        this.deathDate = rs.getDate("death_dt");
        this.latestDischargeDate = rs.getDate("latest_discharge_dt");
        this.ethnicity = rs.getString("ethnicity");
        this.race = rs.getString("race");
        this.ethnicGroup = rs.getString("ethnic_group");
        this.gender = rs.getString("gender");
        this.zipCode = rs.getString("zip_code");
    }

    @XmlAttribute
    public String getBirthDate() {
        return sdf.format(this.birthDate);
    }

    public void setBirthDate(String birthDate) throws ParseException {
        this.birthDate = new Date(sdf.parse(birthDate).getTime());
    }

    @XmlAttribute
    public String getDeathDate() {
        return(this.deathDate == null ? "" : sdf.format(this.deathDate));
    }

    public void setDeathDate(String deathDate) throws ParseException {
        this.deathDate = (deathDate == null || deathDate.length() == 0 ? null : new Date(sdf.parse(deathDate).getTime()));
    }
    
    @XmlAttribute
    public String getLatestDischargeDate() {
        return(this.latestDischargeDate == null ? "" : sdf.format(this.latestDischargeDate));
    }

    public void setLatestDischargeDate(String latestDischargeDate) throws ParseException {
        this.latestDischargeDate = (latestDischargeDate == null || latestDischargeDate.length() == 0 ? null : new Date(sdf.parse(latestDischargeDate).getTime()));
    }

}
