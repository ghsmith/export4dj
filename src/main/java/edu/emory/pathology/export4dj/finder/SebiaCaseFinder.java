package edu.emory.pathology.export4dj.finder;

import edu.emory.pathology.export4dj.data.SebiaCase;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 *
 * @author Geoffrey H. Smith
 * 
 * The Sebia Phoresis system uses a PostgreSQL database. I would prefer to
 * connect directly to that database, as I am doing with the Clinical Data
 * Warehouse Oracle database and the Cerner CoPathPlus MS SQL Server database.
 * However, our Sebia Phoresis system is not on the corporate data network,
 * so this finder is designed to work with CSV files that are manually dumped
 * from the PostgreSQL database using statements of the following form. Note
 * that the database uses Italian names.
 * 
 * copy
 * (
 *   select
 *     *
 *   from
 *     anagrafica
 *   where
 *     data_analisi >= '01/01/2019'
 *     and data_analisi < '01/01/2020'
 * )
 * to sebia.csv with(format csv, header);
 * 
 */
public class SebiaCaseFinder {

    public Map<String, CSVRecord> csvRecordsByAccNo;
    public Map<String, CSVRecord> csvNormalControlRecordsByRunDate;
    
    public SebiaCaseFinder(File csvIn) throws IOException {
        
        CSVParser csvParser = CSVParser.parse(csvIn, Charset.defaultCharset(), CSVFormat.DEFAULT.withFirstRecordAsHeader());

        csvRecordsByAccNo = new HashMap<>();
        csvNormalControlRecordsByRunDate = new HashMap<>();
        for(CSVRecord csvRecord : csvParser) {
            if(("B".equals(csvRecord.get("programma")) || "R".equals(csvRecord.get("programma"))) && csvRecord.get("id") != null) {
                if(csvRecord.get("id").length() == 9) {
                    // PathNet Classic accession number (minus year) and plus
                    // container
                    String collectionYear = csvRecord.get("data_analisi").substring(2, 4);
                    if(csvRecord.get("data_prel") != null && csvRecord.get("data_prel").length() > 0) { // this is the actual collection date, but it isn't always there
                        collectionYear = csvRecord.get("data_prel").substring(2, 4);
                    }
                    csvRecordsByAccNo.put(collectionYear + csvRecord.get("id").substring(0,8), csvRecord);
                }
                else if(csvRecord.get("id").length() == 13) {
                    // Millennium PathNet accession number (minus decade) and
                    // plus container
                    csvRecordsByAccNo.put("00" + csvRecord.get("id").substring(0, 3) + "201" + csvRecord.get("id").substring(3, 7) + "0" + csvRecord.get("id").substring(7, 12), csvRecord);
                }
                if(csvRecord.get("nominativo").contains("#")) {
                    csvNormalControlRecordsByRunDate.put(csvRecord.get("data_analisi") + "." + csvRecord.get("programma") + "." + csvRecord.get("seq"), csvRecord);
                }
            }
        }

    }

   public SebiaCase getSebiaCaseByAccNo(String accNo) {
       SebiaCase sebiaCase = null;
       if(csvRecordsByAccNo.get(accNo) != null) {
           sebiaCase = new SebiaCase(csvRecordsByAccNo.get(accNo));
       }
       return sebiaCase;
   }

   public SebiaCase getSebiaNormalControlCaseByAccNo(String accNo) {
       SebiaCase sebiaCaseNormalControl = null;
       if(csvRecordsByAccNo.get(accNo) != null) {
           for(int seq = Integer.parseInt(csvRecordsByAccNo.get(accNo).get("seq")); seq >= 1; seq--) {
               CSVRecord csvRecord = csvNormalControlRecordsByRunDate.get(csvRecordsByAccNo.get(accNo).get("data_analisi") + "." + csvRecordsByAccNo.get(accNo).get("programma") + "." + seq);
               if(csvRecord != null) {
                   sebiaCaseNormalControl = new SebiaCase(csvRecord);
                   break;
               }
           }
       }
       return sebiaCaseNormalControl;
   }
   
}