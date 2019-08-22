package com.littlely.mr.table;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {

    private String order_id; // 订单id
    private String p_id; // 产品id
    private int amount; // 产品数量
    private String pName; // 产品名称
    private String flag; // 表的标记

    public TableBean(){
        super();
    }

    public TableBean(String order_id, String p_id, int amount, String pName, String flag){

        super();
        this.order_id = order_id;
        this.p_id = p_id;
        this.amount = amount;
        this.pName = pName;
        this.flag = flag;

    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(order_id);
        out.writeUTF(p_id);
        out.writeInt(amount);
        out.writeUTF(pName);
        out.writeUTF(flag);
    }

    public void readFields(DataInput in) throws IOException {

        this.order_id = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readInt();
        this.pName = in.readUTF();
        this.flag = in.readUTF();

    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {

        return order_id + "\t" + pName + "\t" + amount + "\t";
    }
}
