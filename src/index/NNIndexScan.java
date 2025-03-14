package index;

import LSHFIndex.LSHFIndexFile;
import bufmgr.PageNotReadException;
import global.*;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import iterator.*;

import java.io.IOException;

public class NNIndexScan extends Iterator {

    private IndexType indexType;
    private String relName;
    private String indexName;

    private AttrType[] attrTypes;
    private short[] str_sizes;

    private int noInFlds;
    private int noOutFlds;
    private FldSpec[] outFlds;
    private CondExpr[] condExprs;
    private int fldNum;
    private Vector100Dtype query;
    private int count;

    private Tuple jTuple;
    private Tuple tuple1;
    private int t1_size;
    private Heapfile f;
    private LSHFIndexFile indexFile;
    private Sort sortedK;
    public NNIndexScan(IndexType indexType, String relName, String indexName, AttrType[] types, short[] str_sizes,int noInFlds, int noOutFlds, FldSpec[] outFlds, CondExpr[] condExprs, int fldNum, Vector100Dtype query, int count) throws IndexException, UnknownIndexTypeException {
        this.indexType = indexType;
        this.relName = relName;
        this.indexName = indexName;
        this.attrTypes = types;
        this.str_sizes = str_sizes;
        this.noInFlds = noInFlds;
        this.noOutFlds = noOutFlds;
        this.outFlds = outFlds;
        this.condExprs = condExprs;
        this.fldNum = fldNum;
        this.query = query;
        this.count = count;

        AttrType[] Jtypes = new AttrType[noOutFlds];
        short[] tsSizes;
        jTuple = new Tuple();

        try{
            tsSizes = TupleUtils.setup_op_tuple(jTuple, Jtypes, types, noInFlds, str_sizes, outFlds, noOutFlds);
        } catch (InvalidRelation e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TupleUtilsException e) {
            throw new RuntimeException(e);
        }

        tuple1 = new Tuple();
        try {
            tuple1.setHdr((short) noInFlds, types, str_sizes);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile error");
        }
        t1_size = tuple1.size();

        try {
            f = new Heapfile(relName);
        } catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: Heapfile not created");
        }

        switch (indexType.indexType) {
            // linear hashing is not yet implemented
            case IndexType.LSHFIndex:
                // error check the select condition
                // must be of the type: value op symbol || symbol op value
                // but not symbol op symbol || value op value
                try {
                    indexFile = new LSHFIndexFile(indexName);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from BTreeFile constructor");
                }

                try {
                    sortedK = indexFile.nearestNeighbourScan(query, count);
                } catch (Exception e) {
                    System.err.println(e);
                    throw new IndexException(e, "NNScan.java: exception caught in the NNScan constructor");
                }

                break;

            case IndexType.None:
            default:
                throw new UnknownIndexTypeException("Only BTree index is supported so far");

        }
    }
    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        Tuple t;
        RID rid;
        try{
            t = sortedK.get_next();
        }catch (Exception e) {
            throw new IndexException(e, "IndexScan.java: BTree error");
        }
        if (t!= null){
            t.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
            rid = new RID(new PageId(t.getIntFld(2)), t.getIntFld(3));
            try {
                tuple1 = f.getRecord(rid);
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: getRecord failed");
            }
            try {
                tuple1.setHdr((short) noInFlds, attrTypes, str_sizes);
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: Heapfile error");
            }
            boolean eval;
            try {
                eval = PredEval.Eval(condExprs, tuple1, null, attrTypes, null);
            } catch (Exception e) {
                throw new IndexException(e, "IndexScan.java: Heapfile error");
            }

            if (eval) {
                // need projection.java
                try {
                    Projection.Project(tuple1, attrTypes, jTuple, outFlds, noOutFlds);
                } catch (Exception e) {
                    throw new IndexException(e, "IndexScan.java: Heapfile error");
                }

                return jTuple;

            }
            return null;
        }
        return null;
    }

    @Override
    public void close() throws IOException, JoinsException, SortException, IndexException {
        sortedK.close();
    }
}
