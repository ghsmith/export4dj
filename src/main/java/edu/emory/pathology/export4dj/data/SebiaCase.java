package edu.emory.pathology.export4dj.data;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author Geoffrey H. Smith
 */
@XmlRootElement
public class SebiaCase {

    @XmlRootElement
    public static class SebiaFraction {
        
        @XmlAttribute
        public String name;
        @XmlAttribute
        public String pct;

        public SebiaFraction(String name, String pct) {
            this.name = name;
            this.pct = pct;
        }
        
        public SebiaFraction() {
        }

        @Override
        public String toString() {
            return String.format("(%s, %s)", name, pct);
        }
        
    }
    
    @XmlAttribute
    public String id;
    @XmlAttribute
    public String protein;
    public String curve;
    public String originalCurve;
    @XmlElementWrapper(name = "fractions")
    @XmlElement(name = "fraction")
    public List<SebiaFraction> sebiaFractions;

    public SebiaCase(CSVRecord csvRecord) {
        this.id = csvRecord.get("id");
        this.protein = csvRecord.get("pt");
        this.curve = csvRecord.get("curva");
        this.originalCurve = csvRecord.get("originalcurve");
        this.sebiaFractions = new ArrayList<>();
        for(int x = 1; x <= Integer.parseInt(csvRecord.get("numfraz")); x++) {
            sebiaFractions.add(new SebiaFraction(csvRecord.get("nome_" + x), csvRecord.get("fraz_" + x)));
        }
    }

    public SebiaCase() {
    }    
}
