package LSHFIndex;


import global.AttrType;
import global.Vector100Dtype;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

public class LSHDto implements Comparable<LSHDto> {
    private Vector100Dtype v;
    private int pid;
    private int sid;
    private int hash;

    public LSHDto(Vector100Dtype v, int pid, int sid) {
        this.v = v;
        this.pid = pid;
        this.sid = sid;
    }

    public LSHDto(int hash, int pid) {
        this.hash = hash;
        this.pid = pid;
    }


    public Vector100Dtype getV() {
        return v;
    }
    public int getPid() {
        return pid;
    }
    public int getSid() {
        return sid;
    }
    public int getHash(){
        return hash;
    }
    public Tuple toLeafTuple() throws InvalidTupleSizeException, IOException, InvalidTypeException, FieldNumberOutOfBoundException {
        Tuple tuple = new Tuple();
        tuple.setHdr((short)3, new AttrType[]{new AttrType(AttrType.attrVector100D), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)}, null);
        tuple.set100DVectFld(1,this.v);
        tuple.setIntFld(2, pid);
        tuple.setIntFld(3, sid);
        return tuple;
    }

    @Override
    public int compareTo(LSHDto o) {
        return Integer.compare(this.getHash(), o.getHash());
    }
}
