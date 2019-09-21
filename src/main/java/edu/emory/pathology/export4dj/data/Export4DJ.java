package edu.emory.pathology.export4dj.data;

import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author Geoffrey H. Smith
 */
@XmlRootElement
public class Export4DJ {
    
    @XmlElementWrapper(name = "coPathCases")
    @XmlElement(name = "coPathCase")
    public List<CoPathCase> coPathCases;
    
}
