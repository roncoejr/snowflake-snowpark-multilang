package com.roncoejr;

import java.io.Serializable;


public class HalfDuration implements Serializable {
    public static void main(String[] args) {
        System.out.println("Some initialization code might be here.");
    }
    public static String cutInHalf(String x) {
        // System.out.println(Float.parseFloat(x) / 2);
        return String.valueOf((Float.parseFloat(x) / 2));
    }

    public static String rateDistance(String x) {
        String statement = "";
        Float nRes = Float.parseFloat(x);

        if (nRes <= 5.0) {
            statement = "Short Distance";
        }

        if (nRes > 5.0 && nRes <= 15.0) {
            statement = "Medium Distance";
        }

        if (nRes >15) {
            statement = "That's a long way....";
        }

        return statement;
    }
}
