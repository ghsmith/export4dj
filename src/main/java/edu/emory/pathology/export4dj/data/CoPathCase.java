package edu.emory.pathology.export4dj.data;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

/**
 *
 * @author Geofrey H. Smith
 */
public class CoPathCase {

    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy"); 
    
    public String specimenId;
    public String accNo;
    public Date accessionDate;
    public Date collectionDate;
    public String empi;

    public CoPathCase(ResultSet rs) throws SQLException {
        this.specimenId = rs.getString("specimen_id");
        this.accNo = rs.getString("specnum_formatted");
        this.accessionDate = rs.getDate("accession_date");
        this.collectionDate = rs.getDate("datetime_taken");
        this.empi = rs.getString("universal_mednum_stripped");
    }

    @Override
    public String toString() {
        return(String.format(
            "%s,%s,%s,%s,%s",
            specimenId,
            accNo,
            sdf.format(accessionDate),
            sdf.format(collectionDate),
            empi
        ));
    }
    
}
