package cn.itstudy.demo2.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * author: DahongZhou
 * Date:
 */
public class PairSort implements WritableComparable<PairSort> {
    private String first;
    private Integer second;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return "PairSort{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }

    public PairSort() {
    }

    public PairSort(String first, int second) {
        this.set(first, second);
    }

    /**
     * 方便设置字段
     */
    public void set(String first, int second) {
        this.first = first;
        this.second = second;
    }


    //反序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);

    }

    //序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }

    @Override
    public int compareTo(PairSort o) {
        //比较第一列数据
        int i = this.first.compareTo(o.first);
        if (i != 0) {//i不等于0，说明第一列不等
            //如果第一列不相等，优先用第一列排序
            return i;
        } else {
            //如果第一列相等， a    a
            //比较第二列
            int i1 = this.second.compareTo(o.second);

            //如果第一列相等了，比较第二列，直接将第二列的值返回，
            return i1;
        }
    }


}
