package com.bigdata.join.mapjoin;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class TableBean implements WritableComparable<TableBean> {

    private String orderId;

    private String pId;

    private int amonut;

    private String pName;

    private int flag;

    public TableBean() {

    }

    public int compareTo(TableBean o) {
        return 0;
    }

    // 序列化
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.pId);
        dataOutput.writeInt(this.amonut);
        dataOutput.writeUTF(this.pName);
        dataOutput.writeInt(this.flag);
    }

    // 反序列化
    public void readFields(DataInput dataInput) throws IOException {

        this.orderId = dataInput.readUTF();
        this.pId = dataInput.readUTF();
        this.amonut = dataInput.readInt();
        this.pName = dataInput.readUTF();
        this.flag = dataInput.readInt();
    }

    public String toString() {
        return this.orderId + "\t" + this.pName + "\t" + this.amonut;
    }
}
