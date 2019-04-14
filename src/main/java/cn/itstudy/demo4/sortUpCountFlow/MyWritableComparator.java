package cn.itstudy.demo4.sortUpCountFlow;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * author: DahongZhou
 * Date:
 */
public class MyWritableComparator extends WritableComparator {
    //重写无参构造器，指定框架反射出来的两个比较类型是OrderBean类
    public MyWritableComparator() {
        super(FlowBeanSort.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FlowBeanSort first = (FlowBeanSort) a;
        FlowBeanSort second = (FlowBeanSort) b;
        return first.toString().compareTo(second.toString());
    }
}
