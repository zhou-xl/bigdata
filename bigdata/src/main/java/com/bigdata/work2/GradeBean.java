package com.bigdata.work2;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
public class GradeBean implements WritableComparable<GradeBean> {

    /**
     * 姓名
     */
    private String name;

    /**
     * 成绩
     */
    private int score;

    public GradeBean() {

    }
    @Override
    public int compareTo(GradeBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeInt(this.score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.score = dataInput.readInt();
    }
}
