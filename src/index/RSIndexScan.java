package index;

import global.*;
import bufmgr.*;
import diskmgr.*;
import iterator.*;
import heap.*;
import java.io.*;
import java.util.Iterator;

/**
 * RSIndexScan class performs a range search over an LSH-based index.
 * It retrieves all tuples whose vectors are within a specified radius
 * of the query vector, applies selection conditions, and projects the
 * required fields.
 */
public class RSIndexScan extends Iterator {

    private LSHFIndexFile indexFile;       // The LSH index file
    private Iterator rangeResults;         // Iterator over range scan results
    private Heapfile heapFile;             // Heap file with full tuples
    private Tuple currentTuple;            // Current tuple from range scan
    private Tuple outputTuple;             // Projected output tuple
    private CondExpr[] selections;         // Selection conditions
    private FldSpec[] projection;          // Fields to project
    private int numOutFields;              // Number of fields in output tuple
    private AttrType[] attrTypes;          // Attribute types of input tuple
    private short[] strSizes;              // String sizes for string attributes
    private boolean indexOnly;             // Flag for index-only scan

    /**
     * Constructor for RSIndexScan.
     *
     * @param index      Type of the index (must be LSHFIndex)
     * @param relName    Name of the input relation (heap file)
     * @param indName    Name of the input index file
     * @param types      Array of attribute types in the relation
     * @param str_sizes  Array of string sizes for string attributes
     * @param noInFlds   Number of fields in input tuple
     * @param noOutFlds  Number of fields in output tuple
     * @param outFlds    Fields to project (FldSpec array)
     * @param selects    Selection conditions (CondExpr array)
     * @param fldNum     Field number of the indexed vector field
     * @param query      Query vector for the range search
     * @param radius     Radius for the range search
     * @param indexOnly  Whether to return only the index key (vector) or full tuple
     * @throws Exception if initialization fails
     */
    public RSIndexScan(
            IndexType index,
            final String relName,
            final String indName,
            AttrType[] types,
            short[] str_sizes,
            int noInFlds,
            int noOutFlds,
            FldSpec[] outFlds,
            CondExpr[] selects,
            final int fldNum,
            Vector100Dtype query,
            double radius,
            final boolean indexOnly
    ) throws Exception {
        // Validate index type
        if (!index.equals(new IndexType(IndexType.LSHFIndex))) {
            throw new Exception("RSIndexScan requires LSHFIndex type");
        }

        // Initialize member variables
        this.indexFile = new LSHFIndexFile(indName);
        this.heapFile = new Heapfile(relName);
        this.selections = selects;
        this.projection = outFlds;
        this.numOutFields = noOutFlds;
        this.attrTypes = types;
        this.strSizes = str_sizes;
        this.indexOnly = indexOnly;

        // Set up the output tuple header
        AttrType[] outAttrTypes = new AttrType[noOutFlds];
        short[] outStrSizes = new short[noOutFlds]; // Adjust if needed
        int strSizeIdx = 0;
        for (int i = 0; i < noOutFlds; i++) {
            int offset = outFlds[i].offset - 1;
            outAttrTypes[i] = types[offset];
            if (types[offset].attrType == AttrType.attrString && strSizeIdx < str_sizes.length) {
                outStrSizes[i] = str_sizes[strSizeIdx++];
            }
        }
        this.outputTuple = new Tuple();
        this.outputTuple.setHdr((short) noOutFlds, outAttrTypes, outStrSizes);

        // Start the range scan
        this.rangeResults = indexFile.rangeScan(query, radius);
    }

    /**
     * Retrieves the next tuple in the range that satisfies the conditions.
     *
     * @return The next tuple, or null if no more tuples are available
     * @throws Exception if tuple retrieval or evaluation fails
     */
    public Tuple get_next() throws Exception {
        while (rangeResults.hasNext()) {
            currentTuple = (Tuple) rangeResults.next();
            if (currentTuple == null) continue;

            // Extract RID from the result tuple (assumed fields: Vector100D, PageId, SlotNo)
            int pageId = currentTuple.getIntFld(2); // Field 2: PageId
            int slotNo = currentTuple.getIntFld(3); // Field 3: SlotNo
            RID rid = new RID(new PageId(pageId), slotNo);

            if (indexOnly) {
                // For index-only scans, return only the vector
                outputTuple.setVectorFld(1, currentTuple.getVectorFld(1));
                return outputTuple;
            }

            // Fetch the full tuple from the heap file
            Tuple fullTuple = heapFile.getRecord(rid);
            if (fullTuple == null) continue;

            // Apply selection conditions if any
            if (selections != null && !PredEval.Eval(selections, fullTuple, null, attrTypes, null)) {
                continue; // Skip if selection fails
            }

            // Project the required fields into outputTuple
            for (int i = 0; i < numOutFields; i++) {
                int offset = projection[i].offset;
                switch (attrTypes[offset - 1].attrType) {
                    case AttrType.attrInteger:
                        outputTuple.setIntFld(i + 1, fullTuple.getIntFld(offset));
                        break;
                    case AttrType.attrReal:
                        outputTuple.setFloFld(i + 1, fullTuple.getFloFld(offset));
                        break;
                    case AttrType.attrString:
                        outputTuple.setStrFld(i + 1, fullTuple.getStrFld(offset));
                        break;
                    case AttrType.attrVector: // Assuming a vector type exists
                        outputTuple.setVectorFld(i + 1, fullTuple.getVectorFld(offset));
                        break;
                    default:
                        throw new Exception("Unsupported attribute type in projection");
                }
            }

            return outputTuple;
        }
        return null; // No more tuples
    }

    /**
     * Closes the index scan and releases resources.
     *
     * @throws Exception if closing fails
     */
    public void close() throws Exception {
        if (indexFile != null) {
            indexFile.close();
            indexFile = null;
        }
        if (rangeResults != null) {
            rangeResults = null;
        }
        // Additional cleanup if needed (e.g., heapFile)
    }
}