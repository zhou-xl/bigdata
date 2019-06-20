package com.bigdata.writable;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class FlowBean implements WritableComparable<FlowBean> {

    // 上行
    private long upflow;

    // 下行
    private long downflow;

    // 总流量
    private long sumflow;

    // 反序列化
    public FlowBean() {

    }

    public int compareTo(FlowBean o) {


        return this.sumflow > o.sumflow ? -1 : 1;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeLong(this.upflow);
        dataOutput.writeLong(this.downflow);
        dataOutput.writeLong(this.sumflow);
    }

    public void readFields(DataInput dataInput) throws IOException {

        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumflow = dataInput.readLong();
    }

    public String toString() {
        return this.upflow + "\t" + this.downflow + "\t" + this.sumflow;
    }
}
