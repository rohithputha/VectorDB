package query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import LSHFIndex.LSHDto;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.HFPage;
import heap.Heapfile;
import heap.Tuple;
import index.NNIndexScan;
import index.RSIndexScan;
import iterator.*;

public class testquery {
    public static void main(String[] args) throws Exception {
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
        SystemDefs sysdef = new SystemDefs(dbName, 0, numBuf, "Clock");
        BufferedReader br = new BufferedReader(new FileReader(qsName));
        String query = br.readLine();
        br.close();
        String[] params = parseQueryParams(query, "NN");
        int QA = Integer.parseInt(params[0]);
                //System.out.println(QA);
        BufferedReader bufr = new BufferedReader(new FileReader(params[1] + ".txt"));
        String vec = bufr.readLine();
        bufr.close();
        String[] veclist = vec.trim().split("\\s+");
        Vector100Dtype targetVect = new Vector100Dtype();
        for (int i = 0; i < 100; i++) {
            targetVect.set(i, Short.parseShort(veclist[i]));
        }
        //targetVect.showvec();
        int k = Integer.parseInt(params[2]);

        PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + dbName);
        HFPage page = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pid, page, false);
        Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
        m.setHdr((short) 1,
                new AttrType[]{new AttrType(AttrType.attrInteger)},
                null);
        int input_fields_length = m.getIntFld(1);

        AttrType[] intattrarray = new AttrType[input_fields_length];

        for (int i = 0; i < input_fields_length; i++) {
            //int typeInt = Integer.parseInt(typeStrs[i]);
            intattrarray[i] = new AttrType(AttrType.attrInteger);
        }

        Tuple f = page.getRecord(new RID(page.getCurPage(), 1));
        f.setHdr((short) input_fields_length,
                intattrarray,
                null);

        AttrType[] schemaAttrTypes = new AttrType[input_fields_length];


        for (int i = 0; i < input_fields_length; i++) {
            int fldtype = f.getIntFld(i + 1);
            if (fldtype == 1) {
                schemaAttrTypes[i] = new AttrType(AttrType.attrInteger);
            } else if (fldtype == 2) {
                schemaAttrTypes[i] = new AttrType(AttrType.attrReal);
            } else if (fldtype == 3) {
                schemaAttrTypes[i] = new AttrType(AttrType.attrString);
            } else if (fldtype == 4) {
                schemaAttrTypes[i] = new AttrType(AttrType.attrVector100D);
            }
        }

        SystemDefs.JavabaseBM.unpinPage(pid, false);

        PageId pidhL = SystemDefs.JavabaseDB.get_file_entry("handL" + dbName);
        HFPage page_x = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pidhL, page_x, false);
        Tuple x = page_x.getRecord(new RID(page_x.getCurPage(), 0));
        x.setHdr((short) 2,
                new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                null);
        int h = x.getIntFld(1);
        int L = x.getIntFld(2);

        SystemDefs.JavabaseBM.unpinPage(pidhL, false);
        String index_name = dbName + "_" + Integer.toString(QA - 1) + "_" + Integer.toString(L) + "_" + Integer.toString(h);
        int output_fields_length = input_fields_length;

        FldSpec[] outFldstack = new FldSpec[output_fields_length];
        for (int i = 0; i < output_fields_length; i++) {
            outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }


        NNIndexScan nnIndexScan = new NNIndexScan(
                new IndexType(IndexType.LSHFIndex),
                dbName,
                index_name,
                schemaAttrTypes,
                null,
                input_fields_length,
                output_fields_length,
                outFldstack,
                null,
                QA,
                targetVect,
                k);

        while (true) {
            Tuple t = nnIndexScan.get_next();
            if (t != null) {
                t.setHdr((short) input_fields_length, schemaAttrTypes, null);
                //System.out.println(t.getFloFld(1));
                printer(params, schemaAttrTypes, t);
            } else break;
        }

        nnIndexScan.close();


        
        
        SystemDefs.JavabaseDB.closeDB();
        
    }
    private static String[] parseQueryParams(String queryLine, String queryType) {
        // Extract content between parentheses
        int startIdx = queryLine.indexOf('(') + 1;
        int endIdx = queryLine.lastIndexOf(')');
        String paramsStr = queryLine.substring(startIdx, endIdx);

        // Split by commas, handling potential spaces
        return paramsStr.split("\\s*,\\s*");
    }

    private static void printer(String[] params, AttrType[] types, Tuple t) throws FieldNumberOutOfBoundException, IOException {
        for (int i = 3; i < params.length; i++) {
            int type = types[Integer.parseInt(params[i]) - 1].attrType;
            switch (type) {
                case 1:
                    System.out.println(t.getIntFld(Integer.parseInt(params[i])));
                    break;
                case 2:
                    System.out.println(t.getFloFld(Integer.parseInt(params[i])));
                    break;
                case 3:
                    System.out.println(t.getStrFld(Integer.parseInt(params[i])));
                    break;

                case 5:
                    System.out.println(Arrays.toString(t.get100DVectFld(Integer.parseInt(params[i])).getVector()));
                    break;
            }
        }

        System.out.println("---------------------------");
    }
}



