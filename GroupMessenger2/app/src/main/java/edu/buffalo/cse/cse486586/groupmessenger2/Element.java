package edu.buffalo.cse.cse486586.groupmessenger2;

/**
 * Created by andi on 3/6/17.
 */

public class Element {

    private String seq;
    private String pid;
    private float pID;
    private String msg;
    private boolean isAgreed;
    private boolean isDeliverable;
    private String agreedSeq;

    Element() {
        msg = null;
        seq = null;

        pID = 0;
        isAgreed = false;
        isDeliverable = false;
        agreedSeq = null;

    }

    void setSeq(String val) {
        seq = val;
    }

    String getSeq() {
        return seq;

    }



    float getPID() {
        return pID;
    }

    String getMsg() {
        return msg;
    }

    boolean getisAgreed() {
        return isAgreed;
    }

    boolean isDeliverable() {
        return isDeliverable;
    }


    void setMsg(String s) {
        msg = s;
    }

    void setPID(float mID) {
        pID = mID;
    }

    void setisAgreed(boolean val) {
        isAgreed = val;
    }

    void setisDeliverable(boolean val) {
        isDeliverable = val;
    }

    void setAgreedSeq(String val) {
        agreedSeq = val;
    }

    String getAgreedSeq() {
        return agreedSeq;
    }

    @Override
    public String toString() {
        return "Element{" +
                "seq='" + seq + '\'' +
                ", pid='" + pid + '\'' +
                ", pID=" + pID +
                ", isAgreed=" + isAgreed +
                ", isDeliverable=" + isDeliverable +
                ", agreedSeq='" + agreedSeq + '\'' +
                ", msg='" + msg + '\'' +
                '}' + "\n";
    }
}
