package com.cs.rfq.decorator.extractors;

public class Ratio {
    //the ratio is a:b
    long a;
    long b;

    public Ratio(long a, long b) {
        if (b == 0) {
            this.a = a;
        } else {
            this.a = a / gcd(a, b);
        }

        if (a == 0) {
            this.b = b;
        }else {
            this.b = b / gcd(a, b);
        }
    }

    public String RatioToString() {
        if (a < 0 || b < 0)
            return String.valueOf(-1);
        return String.valueOf(a) + ":" + String.valueOf(b);
    }

    public long gcd(long p, long q) {
        if (q == 0) return p;
        else return gcd(q, p % q);
    }

}
