package LSHFIndex;


import global.Vector100Dtype;

public class LSHLeafDto {
    private Vector100Dtype v;
    private int pid;
    private int sid;

    public LSHLeafDto(Vector100Dtype v, int pid, int sid) {
        this.v = v;
        this.pid = pid;
        this.sid = sid;
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
