package global;

import java.io.*;
import btree.KeyClass;

public class Vector100DKey extends KeyClass {
    private Vector100Dtype value;
    
    public Vector100DKey(Vector100Dtype value) {
        this.value = value;
    }
    
    public Vector100Dtype getValue() {
        return value;
    }
    
    public void setValue(Vector100Dtype value) {
        this.value = value;
    }
    
    //@Override
    public String toString() {
        return value.toString();
    }
    
    //@Override
    public boolean equals(KeyClass key) {
        if (key instanceof Vector100DKey) {
            Vector100DKey vKey = (Vector100DKey) key;
            return value.equals(vKey.getValue());
        }
        return false;
    }
    
    //@Override
    public int getLength() {
        // Size in bytes: 100 dimensions * 2 bytes per dimension
        return 200;
    }
    
    //@Override
    // public void writeToDataPage(DataOutputStream out) throws IOException {
    //     // Serialize the vector to the data page
    //     for (int i = 0; i < 100; i++) {
    //         out.writeShort(value.getValue(i));
    //     }
    // }
    
    //@Override
    // public void readFromDataPage(DataInputStream in) throws IOException {
    //     // Read the vector from the data page
    //     value = new Vector100Dtype();
    //     for (int i = 0; i < 100; i++) {
    //         value.setValue(i, in.readShort());
    //     }
    // }
}