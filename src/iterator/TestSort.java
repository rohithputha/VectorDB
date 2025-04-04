package iterator;

import global.*;
import heap.*;
import diskmgr.*;
import bufmgr.*;
import java.io.*;

public class TestSort {
    public static void main(String[] args) {
        try {
            // Initialize MiniBase
            String dbpath = "testdb";
            int num_pages = GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
            SystemDefs sysdef = new SystemDefs(dbpath, GlobalConst.MINIBASE_DB_SIZE, num_pages, "Clock");
            System.out.println("MiniBase initialized with " + num_pages + " buffer pages");

            // Create tuples
            AttrType[] types = {new AttrType(AttrType.attrVector100D)};
            Tuple t1 = new Tuple(206);
            t1.setHdr((short)1, types, null);
            short[] vals1 = new short[100];
            for (int i = 0; i < 100; i++) vals1[i] = (short)(i + 1);
            t1.set100DVectFld(1, new Vector100Dtype(vals1));
            System.out.println("t1 field type: " + types[0].attrType);
            short[] t1Vals = t1.get100DVectFld(1).getVector();
            System.out.println("t1 First: " + t1Vals[0] + ", Last: " + t1Vals[99]);

            Tuple t2 = new Tuple(206);
            t2.setHdr((short)1, types, null);
            short[] vals2 = new short[100];
            for (int i = 0; i < 100; i++) vals2[i] = (short)(i + 10);
            t2.set100DVectFld(1, new Vector100Dtype(vals2));
            System.out.println("t2 field type: " + types[0].attrType);
            short[] t2Vals = t2.get100DVectFld(1).getVector();
            System.out.println("t2 First: " + t2Vals[0] + ", Last: " + t2Vals[99]);

            Tuple t3 = new Tuple(206);
            t3.setHdr((short)1, types, null);
            short[] vals3 = new short[100];
            for (int i = 0; i < 100; i++) vals3[i] = (short)(i + 20);
            t3.set100DVectFld(1, new Vector100Dtype(vals3));
            System.out.println("t3 field type: " + types[0].attrType);
            short[] t3Vals = t3.get100DVectFld(1).getVector();
            System.out.println("t3 First: " + t3Vals[0] + ", Last: " + t3Vals[99]);


            // Create heap file
            Heapfile hf = new Heapfile("test.heap");
            hf.insertRecord(t3.getTupleByteArray());
            hf.insertRecord(t2.getTupleByteArray());
            hf.insertRecord(t1.getTupleByteArray());
            System.out.println("Inserted " + hf.getRecCnt() + " tuples into test.heap");

            // Create FileScan iterator
            FldSpec[] projlist = {new FldSpec(new RelSpec(RelSpec.outer), 1)};
            FileScan fs = new FileScan("test.heap", types, null, (short)1, (short)1, projlist, null);

            // Target vector
            short[] targetVals = new short[100];
            for (int i = 0; i < 100; i++) targetVals[i] = (short)(i + 20);
            Vector100Dtype target = new Vector100Dtype(targetVals);

            // Sort with k=
            Sort sort = new Sort(types, (short)1, null, fs, 1, new TupleOrder(TupleOrder.Ascending), 200, 2, target, 2);
            int count = 0;
            Tuple result;
            while ((result = sort.get_next()) != null && count < 3) {
                short[] resultVals = result.get100DVectFld(1).getVector();
                System.out.println("Result " + (count + 1) + ": First: " + resultVals[0] + ", Last: " + resultVals[99]);
                count++;
            }
            sort.close();

            // Clean up
            fs.close();
            hf.deleteFile();
            SystemDefs.JavabaseDB.closeDB();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Test failed: " + e.getMessage());
        }
    }
}