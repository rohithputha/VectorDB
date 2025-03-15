package LSHFIndex;

import global.RID;
import global.PageId;
import global.Vector100Dtype;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.Sort;
import iterator.TupleUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class LSHFFileRangeScanTest {

    public static void main(String[] args) throws Exception{
        
        String dbpath = "testdb1";
        int num_pages = GlobalConst.MINIBASE_BUFFER_POOL_SIZE;
        SystemDefs sysdef = new SystemDefs(dbpath, num_pages, num_pages, "Clock");
        System.out.println("System defs: " + sysdef);
        Page page = new Page();
        SystemDefs.JavabaseDB.read_page(new PageId(2),page);
        System.out.println(page.getpage());
        System.out.println("MiniBase initialized with " + num_pages + " buffer pages");

        System.out.println("Setting up LSHFIndexFile...");
        LSHFIndexFile indexFile  = new LSHFIndexFile("lshindex",3,3);
        indexFile.close();


        short[] v1Data = new short[100];
        short[] v2Data = new short[100];
        short[] v3Data = new short[100];
        short[] v4Data = new short[100];

        v2Data[0] = 1;
        v2Data[1] = 1;
        v3Data[0] = 2;
        v3Data[1] = 2;
        v4Data[0] = 3;
        v4Data[1] = 3;

        Vector100Dtype v1 = new Vector100Dtype(v1Data);
        Vector100Dtype v2 = new Vector100Dtype(v2Data);
        Vector100Dtype v3 = new Vector100Dtype(v3Data);
        Vector100Dtype v4 = new Vector100Dtype(v4Data);

        System.out.println("Inserting test vectors...");

        indexFile.insert(v1, new RID(new PageId(1), 0));
        System.out.println("Inserted: (" + v1.get(0) + "," + v1.get(1) + ",0,0,..)");

        indexFile.insert(v2, new RID(new PageId(1), 1));
        System.out.println("Inserted: (" + v2.get(0) + "," + v2.get(1) + ",0,0,..)");

        indexFile.insert(v3, new RID(new PageId(1), 2));
        System.out.println("Inserted: (" + v3.get(0) + "," + v3.get(1) + ",0,0,..)");

        indexFile.insert(v4, new RID(new PageId(1), 3));
        System.out.println("Inserted: (" + v4.get(0) + "," + v4.get(1) + ",0,0,..)");

        Vector100Dtype query = new Vector100Dtype(v1Data);
        int radius = 2;
        
        try {
            System.out.println("RangeScan - Q: (" + v1.get(0) + "," + v1.get(1) + ") and R:" + radius + "...");

            Sort results = indexFile.rangeScan(query, radius);
            Tuple tuple;
            int count = 0;
            while ((tuple = results.get_next()) != null) {
                Vector100Dtype v = tuple.get100DVectFld(1);
                int pid = tuple.getIntFld(2);
                int sid = tuple.getIntFld(3);
                int dist = query.distanceTo(v);
                System.out.println("Vector (" + v.get(0) + "," + v.get(1) + "), Distance: " + dist + 
                                 ", PID: " + pid + ", SID: " + sid);
                count++;
            }
            System.out.println("Results found: " + count);
            results.close();

        } catch (Exception e) {
            System.err.println("Exception: " + e.getMessage());
        } finally {
            indexFile.close();
            SystemDefs.JavabaseDB.closeDB();
        }
        
    }
}