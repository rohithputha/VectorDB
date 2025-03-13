package query;

import global.*;

public class MyCommand {
    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: query DBNAME QSNAME INDEXOPTION NUMBUF");
            System.exit(1);
        }

        String dbName = args[0];
        String qsName = args[1];
        String indexOption = args[2];
        int numBuf = Integer.parseInt(args[3]);

        // Validate inputs
        if (!indexOption.equals("Y") && !indexOption.equals("N")) {
            System.out.println("INDEXOPTION must be either 'Y' or 'N'");
            System.exit(1);
        }

        boolean useIndex = indexOption.equals("Y");

        try {
            // Initialize the database system
            SystemDefs sysdef = new SystemDefs(dbName, GlobalConst.MINIBASE_DB_SIZE, numBuf, "Clock");
            System.out.println("Created DB");
            // Process the query
            //processQuery(dbName, qsName, useIndex);
            
            // Output page access statistics
            //printStatistics();
            
            // Clean up
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
