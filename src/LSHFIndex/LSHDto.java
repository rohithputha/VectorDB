package LSHFIndex;


import global.Vector100Dtype;

public class LSHDto {
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
}
