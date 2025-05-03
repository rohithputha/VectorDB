package cli;


import bufmgr.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;
import heap.HFPage;
import heap.Tuple;
import iterator.*;
import org.w3c.dom.Attr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DJoinQueryCommand implements VectorDbCommand {

    private final String query;
    private final String QUERY = "DJoin";
    private String relName1;
    private String relName2;
    private String query1;
    private int qa1;
    private String t;
    private long dist;
    private String i1;
    private int qa2;
    private int dist2;
    private String i2;
    private List<Integer> innerProjList;
    private AttrType[] newAttrs;
    private int allocatedBuff;
    public DJoinQueryCommand(String relName1, String relName2, String query, int allocatedBuff) {
        this.query = query;
        this.relName1 = relName1;
        this.relName2 = relName2;
        this.allocatedBuff = allocatedBuff;
    }

    private String getLshIndexName(String relName, int columnId) throws Exception {
        String fileName = "handL" + relName;
        PageId firstPid = SystemDefs.JavabaseDB.get_file_entry(fileName);
        if (firstPid == null) {
            throw new Exception("No LSH metadata found for relation: " + relName);
        }

        int L = -1, h = -1;
        PageId currentPid = firstPid;
        HFPage hfPage = new HFPage();

        // Scan all pages in the heap‐file for our columnId
        while (currentPid != null) {
            SystemDefs.JavabaseBM.pinPage(currentPid, hfPage, false);
            int slotCount = hfPage.getSlotCnt();
            for (int slot = 0; slot < slotCount; slot++) {
                RID rid = new RID(currentPid, slot);
                Tuple t = hfPage.getRecord(rid);
                // set header so we can read the 3 int fields: [L, h, columnId]
                t.setHdr((short) 3,
                        new AttrType[]{
                                new AttrType(AttrType.attrInteger),
                                new AttrType(AttrType.attrInteger),
                                new AttrType(AttrType.attrInteger)
                        },
                        null);
                if (t.getIntFld(3) == columnId) {
                    h = t.getIntFld(1);
                    L = t.getIntFld(2);
                    SystemDefs.JavabaseBM.unpinPage(currentPid, false);
                    break;
                }
            }
            if (L != -1) break;

            // move to next page (if any)
            PageId next = hfPage.getNextPage();
            SystemDefs.JavabaseBM.unpinPage(currentPid, false);
            currentPid = next;
        }

        if (L == -1) {
            throw new Exception("No metadata entry for column " + columnId +
                    " in relation " + relName);
        }

        // build and return index name
        return relName + "_" + columnId + "_" + L + "_" + h;
    }

    private void parseQueryBody() throws Exception {
        String[] queryBody = this.query.split("[()]");
        String[] args1 = queryBody[2].split(",");
        String[] args2 = queryBody[3].split(",");

        this.query1 = queryBody[1].trim() + "(" + String.join(",", args1) + ")";
        System.out.println(Arrays.toString(args1));

        this.qa2 = Integer.parseInt(args2[1].trim());
        System.out.println(this.qa2);
        this.dist2 = Integer.parseInt(args2[2].trim());
        System.out.println(this.dist2);
        this.i2 = args2[3].trim();
        System.out.println(this.i2);
        this.innerProjList = new ArrayList<>();
        for (int i = 4; i < args2.length; i++) {
            innerProjList.add(Integer.parseInt(args2[i].trim()));
        }
        //rest of argumenbts of arg2 are for  printing out and joining


    }

    @Override
    public String getCommand() {
        return this.query;
    }

    private List<Tuple> useIndex(Iterator outerIterator, AttrType[] outerAttrTypes, int projQa) throws Exception {

        AttrType[] at2 = getSchema(this.relName2);


        FldSpec[] projList = new FldSpec[outerAttrTypes.length + innerProjList.size()];
        for (int i = 0; i < outerAttrTypes.length; i++) {
            projList[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }
        for (int i = outerAttrTypes.length; i < projList.length; i++) {
            projList[i] = new FldSpec(new RelSpec(RelSpec.innerRel), innerProjList.get(i - outerAttrTypes.length));
        }

        INLJoins inlJoin = new INLJoins(
                outerAttrTypes,
                outerAttrTypes.length,
                null,
                at2,
                at2.length,
                null,
                100, ///  CHECK : if this can be configurable
                outerIterator,
                this.relName2,
                new IndexType(IndexType.LSHFIndex),
                getLshIndexName(this.relName2, this.qa2),
                null,
                null,
                projList,
                projList.length,
                this.dist2,
                projQa,
                this.qa2
        );
        List<Tuple> tuples = new ArrayList<>();
        while (true) {
            Tuple t = inlJoin.get_next();
            if (t == null) {
                break;
            }
            tuples.add(t);
        }
        inlJoin.close();
        return tuples;
    }

    private void setNewProjAttrs(AttrType[] outerAttrTypes, AttrType[] innerAttrTypes) throws Exception {
        if (this.newAttrs == null){
            this.newAttrs = new AttrType[outerAttrTypes.length + innerProjList.size()];
            for (int i = 0; i < outerAttrTypes.length; i++) {
                this.newAttrs[i] = outerAttrTypes[i];
            }
            for (int i = 0; i < innerProjList.size(); i++) {
                this.newAttrs[i + outerAttrTypes.length] = innerAttrTypes[this.innerProjList.get(i)-1];
            }
        }

    }

    private Tuple projectAndJoinTuple(Tuple outer, Tuple inner, AttrType[] outerAttrTypes, AttrType[] innerAttrTypes) throws Exception {
        Tuple newT = new Tuple();

        if (this.newAttrs == null){
            setNewProjAttrs(outerAttrTypes, innerAttrTypes);
        }

        newT.setHdr((short) newAttrs.length, newAttrs, null);
        for (int i = 0; i < outerAttrTypes.length; i++) {
            switch (newAttrs[i].attrType) {
                case AttrType.attrInteger:
                    newT.setIntFld(i+1, outer.getIntFld(i+1));
                    break;
                case AttrType.attrReal:
                    newT.setFloFld(i+1, outer.getFloFld(i+1));
                    break;
                case AttrType.attrString:
                    newT.setStrFld(i+1, outer.getStrFld(i+1));
                    break;
                case AttrType.attrVector100D:
                    newT.set100DVectFld(i+1, outer.get100DVectFld(i+1));
            }
        }

        for (int i = 0; i< innerProjList.size(); i++) {
            switch (newAttrs[i+outerAttrTypes.length].attrType) {
                case AttrType.attrInteger:
                    newT.setIntFld(i+outerAttrTypes.length+1, inner.getIntFld(i+1));
                    break;
                case AttrType.attrReal:
                    newT.setFloFld(i+outerAttrTypes.length+1, inner.getFloFld(i+1));
                    break;
                case AttrType.attrString:
                    newT.setStrFld(i+outerAttrTypes.length+1, inner.getStrFld(i+1));
                    break;
                case AttrType.attrVector100D:
                    newT.set100DVectFld(i+outerAttrTypes.length+1, inner.get100DVectFld(i+1));
            }
        }
        return newT;
    }

    private List<Tuple> noIndex(Iterator outerIterator, AttrType[] outerAttrs, int projQa) throws Exception {
        AttrType[] at2 = getSchema(this.relName2);
        FldSpec[] projList = new FldSpec[at2.length];
        for (int i = 0; i < at2.length; i++) {
            projList[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
        }
        List<Tuple> tuples = new ArrayList<>();
        while (true) {
            Tuple tempOuter = outerIterator.get_next();
            if (tempOuter == null) {
                break;
            }
            tempOuter.setHdr((short) outerAttrs.length, outerAttrs, null);

            FileScan fs = new FileScan(this.relName2, at2, null, (short) at2.length, at2.length, projList, null);
            while (true) {
                Tuple tempInner = fs.get_next();
                if (tempInner == null) {
                    break;
                }
                tempInner.setHdr((short) at2.length, at2, null);
//
//                printer(tempInner, at2);
//                System.out.println(this.qa2);
//                System.out.println(projQa);
                if (tempInner.get100DVectFld(this.qa2).distanceTo(tempOuter.get100DVectFld(projQa)) < this.dist2) {
                    tuples.add(projectAndJoinTuple(tempOuter,tempInner, outerAttrs, at2));
                }
            }

        }

        return tuples;

    }

    @Override
    public void process() {
        try {
            parseQueryBody();
            VectorDbCommand vecCommand;
            List<Tuple> tuples;
            Iterator outerIterator = null;
            AttrType[] outerAttrTypes = null;
            int projQa = -1;
            if (this.query1.startsWith("Range(")) {
                vecCommand = new RangeScanQueryCommand(this.relName1, this.relName2, this.query1, true);
                RangeScanQueryCommand rVecCommand = (RangeScanQueryCommand) vecCommand;
                rVecCommand.process();
                outerIterator = (rVecCommand).getIterator();
                outerAttrTypes = (rVecCommand).getProjectAttributeTypes();
                projQa = rVecCommand.getProjQa();

            } else if (this.query1.startsWith("NN(")) {
//                vecCommand = new NNScanQueryCommand;
                vecCommand = new NNScanQueryCommand(this.relName1, this.relName2, this.query1, true, this.allocatedBuff);
                NNScanQueryCommand nVecCommand = (NNScanQueryCommand) vecCommand;
                nVecCommand.process();
                outerIterator = (nVecCommand).getIterator();
                outerAttrTypes = (nVecCommand).getProjectAttributeTypes();
                projQa = nVecCommand.getProjQa();
            }
            this.setNewProjAttrs(outerAttrTypes, getSchema(this.relName2));

//            while (true) {
//                Tuple tempOuter  = outerIterator.get_next();
//                if (tempOuter == null) {
//                    break;
//                }
//                tempOuter.setHdr((short) outerAttrTypes.length, outerAttrTypes, null);
//                printer(tempOuter, outerAttrTypes);
//            }
            if (this.i2.equals("H")) {

                tuples = useIndex(outerIterator, outerAttrTypes, projQa);
            } else {
                printer("not using index for outer join");
                tuples = noIndex(outerIterator, outerAttrTypes, projQa);
            }
            outerIterator.close();
            System.out.println(Arrays.toString(newAttrs));
            System.out.println("------------------------");
            for (Tuple t : tuples) {
                printer(t, newAttrs);
            }

        } catch (Exception e) {
            e.printStackTrace();
            printer(e.getMessage());
        }
    }
}

