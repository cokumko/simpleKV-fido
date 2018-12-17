package core;

import java.util.Arrays;

public class KVPair {

    public char[] element1;
    public char[] element2;

    public KVPair(char[] element1, char[] element2) {
        this.element1 = element1;
        this.element2 = element2;
    }

    @Override
    public boolean equals(Object o) {
        KVPair other = (KVPair) o;
        return Arrays.equals(this.element1, other.element1) && Arrays.equals(this.element2, other.element2);
    }

    @Override
    public String toString() {
        return "(" + charToString(this.element1) + ", " + charToString(this.element2) + ")";
    }

    public static String charToString(char[] arr) {
        if (arr == null || arr.length == 0) return "";
        String result = "";
        for (char c : arr) {
            result += c;
        }
        return result;
    }
}
