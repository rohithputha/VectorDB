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

public class Query {

    private static void printer(String[] params, AttrType[] types, Tuple t) throws FieldNumberOutOfBoundException, IOException {
        for (int i = 3; i < params.length; i++) {
            int type = types[Integer.parseInt(params[i]) - 1].attrType;
            switch (type) {
                case 1:
                    System.out.println(t.getIntFld(Integer.parseInt(params[i])));
                    break;
                case 2:
                    System.out.println(t.getFloFld(Integer.parseInt(params[i])));
                    //System.out.println("Calling Float");
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
        //System.out.println(useIndex);

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


            if (query.startsWith("NN(")) {
                System.out.println("Nearest Neighbour");
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
                //System.out.println(params.length);


                if (useIndex) {
                    PageId pid = SystemDefs.JavabaseDB.get_file_entry("handL" + dbName);
                    HFPage page = new HFPage();
                    SystemDefs.JavabaseBM.pinPage(pid, page, false);
                    Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
                    m.setHdr((short) 3,
                            new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                            null);
                    int h = m.getIntFld(1);
                    int L = m.getIntFld(2);
                    int input_fields_length = m.getIntFld(3);

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
                    //String index_name = "testdb_1_1_1";
                    String index_name = dbName + Integer.toString(QA - 1) + "_" + Integer.toString(h) + "_" + Integer.toString(L);
//                    String index_name = dbName + Integer.toString(QA-1)  + Integer.toString(h)  + Integer.toString(L);

                    // for (int i = 3; i < params.length; i++) {
                    //     System.out.println(params[i]);
                    // }

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
                    // System.out.println(sysdef.h);
                    //System.out.print(output_fields_length);
                    while (true) {
                        Tuple t = nnIndexScan.get_next();
                        if (t != null) {
                            t.setHdr((short) input_fields_length, schemaAttrTypes, null);
                            //System.out.println(t.getFloFld(1));
                            printer(params, schemaAttrTypes, t);
                        } else break;
                    }

                    // for (FldSpec i : outFldstack) {
                    //     System.out.println(i);
                    // }
                    nnIndexScan.close();

                } else {

                    PageId pid = SystemDefs.JavabaseDB.get_file_entry("handL" + dbName);
                    HFPage page = new HFPage();
                    SystemDefs.JavabaseBM.pinPage(pid, page, false);
                    Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
                    m.setHdr((short) 3,
                            new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                            null);
                    int h = m.getIntFld(1);
                    int L = m.getIntFld(2);
                    int input_fields_length = m.getIntFld(3);

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

                    for (int i = 3; i < params.length; i++) {
                        System.out.println(params[i]);
                    }

                    int output_fields_length = params.length - 3;


                    FldSpec[] outFldstack = new FldSpec[input_fields_length];
                    for (int i = 0; i < input_fields_length; i++) {
                        outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                    }

                    //FileScan fs = new FileScan("test.heap", types, null, (short)1, (short)1, projlist, null);
                    // for (AttrType i : schemaAttrTypes) {
                    //     System.out.println(i);
                    // }

                    // for (FldSpec i : outFldstack) {
                    //     System.out.println(i);
                    // }
                    FileScan scan = new FileScan(dbName, schemaAttrTypes, null, (short) input_fields_length, (short) input_fields_length, outFldstack, null);

                    Sort sort = new Sort(schemaAttrTypes, (short) input_fields_length, null, scan, QA, new TupleOrder(TupleOrder.Ascending), 200, 2000, targetVect, k);
                    int count = 0;
                    Tuple result;
                    //System.out.println(QA);
                    for (int x = 0; x < 100; x++) {
                        System.out.print(targetVect.get(x) + ", ");

                    }
                    //System.out.println();
                    while ((result = sort.get_next()) != null && count < k) {
                        printer(params, schemaAttrTypes, result);
                        count++;
                    }
                    scan.close();
                    sort.close();
                    Heapfile dataHeapFile = new Heapfile(dbName);
                    // Scan scan = dataHeapFile.openScan();
                    //System.out.println(dataHeapFile.getRecCnt());
                    // //System.out.println(scan.getNext());
                    // scan.closescan();


                }

                //SystemDefs.JavabaseBM.flushAllPages();
                //SystemDefs.JavabaseDB.closeDB();


            } else if (query.startsWith("Range")) {
                System.out.println("Range Scan");
                String[] params = parseQueryParams(query, "Range");
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
                int d = Integer.parseInt(params[2]);


                if (useIndex) {

                    PageId pid = SystemDefs.JavabaseDB.get_file_entry("handL" + dbName);
                    HFPage page = new HFPage();
                    SystemDefs.JavabaseBM.pinPage(pid, page, false);
                    Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
                    m.setHdr((short) 3,
                            new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                            null);
                    int h = m.getIntFld(1);
                    int L = m.getIntFld(2);
                    int input_fields_length = m.getIntFld(3);

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

                    String index_name = dbName + Integer.toString(QA - 1) + "_" + Integer.toString(h) + "_" + Integer.toString(L);
//                String index_name = dbName + Integer.toString(QA-1) + Integer.toString(h) + Integer.toString(L);


                    // for (int i = 3; i < params.length; i++) {
                    //     System.out.println(params[i]);
                    // }

                    int output_fields_length = input_fields_length;


                    FldSpec[] outFldstack = new FldSpec[output_fields_length];
                    for (int i = 0; i < output_fields_length; i++) {
                        outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                    }

                    RSIndexScan rsIndexScan = new RSIndexScan(
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
                            d
                    );
                    while (true) {
                        Tuple t = rsIndexScan.get_next();
                        if (t != null) {
                            t.setHdr((short) input_fields_length, schemaAttrTypes, null);
                            printer(params, schemaAttrTypes, t);
                        } else break;
                    }
                    rsIndexScan.close();
                    //System.out.println(index_name);
                } else {

                    PageId pid = SystemDefs.JavabaseDB.get_file_entry("handL" + dbName);
                    HFPage page = new HFPage();
                    SystemDefs.JavabaseBM.pinPage(pid, page, false);
                    Tuple m = page.getRecord(new RID(page.getCurPage(), 0));
                    m.setHdr((short) 3,
                            new AttrType[]{new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)},
                            null);
                    int h = m.getIntFld(1);
                    int L = m.getIntFld(2);
                    int input_fields_length = m.getIntFld(3);

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

                    // for (int i = 3; i < params.length; i++) {
                    //     System.out.println(params[i]);
                    // }

                    int output_fields_length = params.length - 3;


                    FldSpec[] outFldstack = new FldSpec[input_fields_length];
                    for (int i = 0; i < input_fields_length; i++) {
                        outFldstack[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
                    }

                    //FileScan fs = new FileScan("test.heap", types, null, (short)1, (short)1, projlist, null);
                    // for (AttrType i : schemaAttrTypes) {
                    //     System.out.println(i);
                    // }

                    // for (FldSpec i : outFldstack) {
                    //     System.out.println(i);
                    // }
                    FileScan scan = new FileScan(dbName, schemaAttrTypes, null, (short) input_fields_length, (short) input_fields_length, outFldstack, null);

//                    Sort sort = new Sort(schemaAttrTypes, (short)input_fields_length, null, scan, QA, new TupleOrder(TupleOrder.Ascending), 200, 2000, targetVect, k);
                    int count = 0;
                    Tuple result;
                    //System.out.println(QA);
                    // for (int x = 0; x < 100; x++) {
                    //     System.out.print(targetVect.get(x) + ", ");

                    // }
                    // System.out.println();
                    while ((result = scan.get_next()) != null) {
                        if (result.get100DVectFld(QA).distanceTo(targetVect) <= d) {
                            printer(params, schemaAttrTypes, result);
                        }

                    }
                    scan.close();

                }
            }
            SystemDefs.JavabaseDB.closeDB();
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

    private static Vector100Dtype createVector(int base) {
        short[] values = new short[100];
        for (int i = 0; i < 100; i++) {
            values[i] = (short) (base);
        }
        return new Vector100Dtype(values);
    }

    public static List<LSHDto> sortByDistance(List<LSHDto> lshdtoList, Vector100Dtype targetVector) {
        // Create a comparator that orders LSHDto objects by their distance from targetVector
        Comparator<LSHDto> distanceComparator = (dto1, dto2) -> {
            double distance1 = dto1.getV().getDistance(targetVector);
            double distance2 = dto2.getV().getDistance(targetVector);
            return Double.compare(distance1, distance2);
        };

        // Sort the list using the comparator
        lshdtoList.sort(distanceComparator);

        // Return the sorted list (which is the same object as the input parameter)
        return lshdtoList;
    }
}
