package com.bigdata.GroupComparator;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class OrderBean implements WritableComparable<OrderBean> {

    private int id;
    private double price;

    public OrderBean() {

    }

    public int compareTo(OrderBean o) {
        if (this.id > o.getId()) {
            return 1;
        } else if (this.id < o.getId()) {
            return -1;
        } else {
            return this.price > o.getPrice() ? -1 : 1;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(this.id);
        dataOutput.writeDouble(this.price);
    }

    public void readFields(DataInput dataInput) throws IOException {

        dataInput.readInt();
        dataInput.readDouble();
    }

    @Override
    public String toString() {
        return this.id + "\t" + this.price;
    }
}
