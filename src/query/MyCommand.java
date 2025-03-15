package query;

import java.io.BufferedReader;
import java.io.FileReader;
import diskmgr.*;
import global.*;
import heap.HFPage;
import heap.Tuple;
import index.NNIndexScan;
import iterator.*;

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
            SystemDefs sysdef = new SystemDefs(dbName, 0, numBuf, "Clock");
            //System.out.println("Created DB");
            // Process the query
            //processQuery(dbName, qsName, useIndex);
            
            // Output page access statistics
            //printStatistics();
            
            // Clean up

            BufferedReader br = new BufferedReader(new FileReader(qsName));
            String query = br.readLine();

            

            if(query.startsWith("NN(")){
                System.out.println("Nearest Neighbour");
                String[] params = parseQueryParams(query, "NN");
                int QA = Integer.parseInt(params[0]);
                //System.out.println(QA);
                BufferedReader bufr = new BufferedReader(new FileReader(params[1]+".txt"));
                String vec = bufr.readLine();
                bufr.close();
                String[] veclist = vec.trim().split("\\s+");
                Vector100Dtype targetVect = new Vector100Dtype();
                for(int i=0;i<100;i++){
                    targetVect.set(i,Short.parseShort(veclist[i]));
                }
                //targetVect.showvec();
                int k = Integer.parseInt(params[2]);
                //System.out.println(params.length);
                

                //String index_name = dbName + (QA-1) + "_" +  
                // PageId pid = SystemDefs.JavabaseDB.get_file_entry("handLmydb");
                // HFPage page = new HFPage();
                // SystemDefs.JavabaseBM.pinPage(pid, page, false);
                // //int pid = Integer.parseInt(pid);
                // // Page pg = new Page();
                // // HFPage page = new HFPage(pg);
                // byte[] bytedata = page.getHFpageArray();
                
                // for(byte i: bytedata){
                //     System.out.println(i);
                // }


                NNIndexScan nnIndexScan = new NNIndexScan(
                new IndexType(IndexType.LSHFIndex),
                dbName,
                "mydb1_5_5",
                new AttrType[]{new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D)},
                null,
                4,
                4,
                new FldSpec[]{new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3), new FldSpec(new RelSpec(RelSpec.outer), 4)},
                null,
                2,
                targetVect,
                k);
                // System.out.println(sysdef.h);
                // System.out.print(sysdef.L);
                while(true){
                    Tuple t = nnIndexScan.get_next();
                    if (t != null) {
                        t.setHdr((short)4, new AttrType[]{new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D),new AttrType(AttrType.attrReal),new AttrType(AttrType.attrVector100D)}, null);
                        Vector100Dtype v = t.get100DVectFld(2);
                        for (int i=0;i<100;i++){
                            System.out.print(v.get(i)+", ");
                        }
                        System.out.println();
                    }
                    else break;
                }
                // for(int i=100;i<4092;i++){
                //     System.out.println(Convert.getIntValue(i, bytedata));
                // }
                //System.out.println(Convert.getIntValue(15, bytedata));
        nnIndexScan.close();


            }
            else if(query.startsWith("Range")){
                System.out.println("Range Scan");
                String[] params = parseQueryParams(query, "NN");
                int QA = Integer.parseInt(params[0]);
                //System.out.println(QA);
                BufferedReader bufr = new BufferedReader(new FileReader(params[1]+".txt"));
                String vec = bufr.readLine();
                bufr.close();
                String[] veclist = vec.trim().split("\\s+");
                Vector100Dtype targetVect = new Vector100Dtype();
                for(int i=0;i<100;i++){
                    targetVect.set(i,Short.parseShort(veclist[i]));
                }
                //targetVect.showvec();
                int d = Integer.parseInt(params[2]);
                System.out.println(params.length);
            }
            else{
                System.out.println("invalid query!");
            }
            //SystemDefs.JavabaseBM.flushAllPages();
            //SystemDefs.JavabaseDB.closeDB();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    private static String[] parseQueryParams(String queryLine, String queryType) {
        // Extract content between parentheses
        int startIdx = queryLine.indexOf('(') + 1;
        int endIdx = queryLine.lastIndexOf(')');
        String paramsStr = queryLine.substring(startIdx, endIdx);
        
        // Split by commas, handling potential spaces
        return paramsStr.split("\\s*,\\s*");
    }
}
