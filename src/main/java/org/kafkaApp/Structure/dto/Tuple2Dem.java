package org.kafkaApp.Structure.dto;

public class Tuple2Dem<A, B> {
    private  A value1;
    private  B value2;
    public Tuple2Dem(){}
    public Tuple2Dem(A value1, B value2) {
        this.value1 = value1;
        this.value2 = value2;
    }

    public A getValue1() {
        return value1;
    }

    public B getValue2() {
        return value2;
    }
}

