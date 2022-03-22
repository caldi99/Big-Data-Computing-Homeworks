import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class G0018 {

    /**
    Receive as input 3 parameters :
    K : Number of partitions
    H : Number of the most popular products
    S : country (all = all country)
    File path

    * */
    public static void main(String[] args) throws IOException
    {
        if (args.length != 4)
            throw new IllegalArgumentException("The Number of parameters is not correct");

        SparkConf conf = new SparkConf(true).setAppName("Homework1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        int K = Integer.parseInt(args[0]);
        int H = Integer.parseInt(args[1]);
        String S = args[2];
        String path = args[3];


        JavaRDD<String> rowData = sc.textFile(path).repartition(K).cache();



    }

}
