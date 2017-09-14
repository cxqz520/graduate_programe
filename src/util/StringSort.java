package util;

/**
 * Created by Orange on 2017/5/30.
 */
public class StringSort implements Comparable<StringSort> {


    private int comparator;
    private String str;

    public StringSort(String str,String spilt,int index){
        this.str = str;
        this.comparator = Integer.parseInt(str.split(spilt)[index]);
    }

    public String toString(){
        return str;
    }


    @Override
    public int compareTo(StringSort o) {
        return  o.comparator -this.comparator;
    }
}
