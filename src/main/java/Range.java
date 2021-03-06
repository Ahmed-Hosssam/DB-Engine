import java.io.Serializable;

public class Range implements Serializable {
    private Comparable minVal, maxVal;

    public Range(Comparable minVal, Comparable maxVal) {
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    public void setMinVal(Comparable minVal) {
        this.minVal = minVal;
    }

    public void setMaxVal(Comparable maxVal) {
        this.maxVal = maxVal;
    }

    public Comparable getMinVal() {
        return minVal;
    }

    public Comparable getMaxVal() {
        return maxVal;
    }


    @Override
    public String toString() {
        return "Range{" +
                "minVal=" + minVal +
                ", maxVal=" + maxVal +
                '}';
    }
}