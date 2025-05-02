package cli;

import bufmgr.*;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.AttrType;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public interface VectorDbCommand {
    String getCommand();

    void process();

    Environment env = new Environment();
    Scanner scanner = new Scanner(System.in);

    default void printer(String message) {
        System.out.print("> ");
        System.out.println(message);
    }

    default void printer(Tuple t, AttrType[] types) throws FieldNumberOutOfBoundException, IOException {
        // expected that tuple has the hdr set with types
        System.out.print("> ");
        for (int i = 0; i < types.length; i++) {

            switch (types[i].attrType) {
                case AttrType.attrInteger:
                    System.out.print(Integer.toString(t.getIntFld(i + 1)));
                    break;
                case AttrType.attrString:
                    System.out.print(t.getStrFld(i + 1));
                    break;
                case AttrType.attrReal:
                    System.out.print(Float.toString(t.getFloFld(i + 1)));
                    break;
                case AttrType.attrVector100D:
                    System.out.print(Arrays.toString(t.get100DVectFld(i + 1).getVector()));
                    break;
            }
            System.out.print("  ||  ");
        }
        System.out.println();
    }

    default void printer() {
        System.out.print("VectorDB> ");
    }

    default Environment getEnvironment() {
        return env;
    }

    default String receive() {
        return scanner.nextLine().trim();
    }

    default void close() {
        scanner.close();
    }

    default AttrType[] getSchema(String relName) throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException, BufferPoolExceededException, PageNotReadException, HashOperationException, BufMgrException, PagePinnedException, InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
        PageId pid = SystemDefs.JavabaseDB.get_file_entry("metadata" + relName);
        if (pid == null) {
            throw new FileNotFoundException("No metadata found for relation " + relName);
        }

        // pin the first (and only) page of the metadata heap file
        HFPage hfPage = new HFPage();
        SystemDefs.JavabaseBM.pinPage(pid, hfPage, false);

        try {
            // --- first record gives numAttributes ---
            RID rid0 = hfPage.firstRecord();
            if (rid0 == null)
                throw new IOException("Empty metadata page for " + relName);

            Tuple t0 = hfPage.getRecord(rid0);
            t0.setHdr((short) 1, new AttrType[]{new AttrType(AttrType.attrInteger)}, null);
            int numAttributes = t0.getIntFld(1);

            // --- second record gives the type‐codes ---
            RID rid1 = hfPage.nextRecord(rid0);
            if (rid1 == null)
                throw new IOException("Missing type‐record in metadata for " + relName);

            Tuple t1 = hfPage.getRecord(rid1);
            AttrType[] types = new AttrType[numAttributes];
            for (int i = 0; i < numAttributes; i++) {
                types[i] = new AttrType(AttrType.attrInteger);
            }
            t1.setHdr((short) numAttributes, types, null);
            AttrType[] schema = new AttrType[numAttributes];

            for (int i = 0; i < numAttributes; i++) {
                switch (t1.getIntFld(i + 1)) {
                    case 1:
                        schema[i] = new AttrType(AttrType.attrInteger);
                        break;
                    case 2:
                        schema[i] = new AttrType(AttrType.attrReal);
                        break;
                    case 3:
                        schema[i] = new AttrType(AttrType.attrString);
                        break;
                    case 4:
                        schema[i] = new AttrType(AttrType.attrVector100D);
                        break;

                }
            }

            return schema;
        } catch (InvalidSlotNumberException e) {
            throw new RuntimeException(e);
        } catch (FieldNumberOutOfBoundException e) {
            throw new RuntimeException(e);
        } catch (InvalidTupleSizeException e) {
            throw new RuntimeException(e);
        } catch (InvalidTypeException e) {
            throw new RuntimeException(e);
        }

    }
}
