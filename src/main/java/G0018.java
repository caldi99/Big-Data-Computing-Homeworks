import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
/*

1. Reads the input file into an RDD of strings called rawData (each 8-field row is read as a single string), and subdivides it into K partitions.
2. Transforms rawData into an RDD of (String,Integer) pairs called productCustomer, which contains all distinct pairs (P,C) such that rawData 
   contains one or more strings whose constituent fields satisfy the following conditions : ProductID=P and CustomerID=C, Quantity>0, and Country=S. 
   If S="all", no condition on Country is applied. IMPORTANT: since the dataset can be potentially very large, the rows relative to a given product P 
   might be too many and you must not gather them together; however, you can safely assume that the rows relative to a given product P and a given customer C 
   are not many (say constant number). Also, although the RDD interface offers a method distinct() to remove duplicates, we ask you to avoid using this method for 
   this step.
3. Uses the mapPartitionsToPair/mapPartitions method to transform productCustomer into an RDD of (String,Integer) pairs called productPopularity1 which, for each 
   product ProductID contains one pair (ProductID, Popularity), where Popularity is the number of distinct customers from Country S (or from all countries if S="all") 
   that purchased a positive quantity of product ProductID. IMPORTANT: in this case it is safe to assume that the amount of data in a partition is small enough to be 
   gathered together.
4. Repeats the operation of the previous point using a combination of map/mapToPair and reduceByKey methods (instead of mapPartitionsToPair/mapPartitions) and calling 
   the resulting RDD productPopularity2.
   (This step is executed only if H>0) Saves in a list and prints the ProductID and Popularity of the H products with highest Popularity. Extracts these data from productPopularity1. 
   Since the RDD can be potentially very large you must not spill the entire RDD onto a list and then extract the top-H products. Check the guide Introduction to Programming in Spark
   to find useful methods to efficiently extract top-valued elements from an RDD.
   (This step, for debug purposes, is executed only if H=0) Collects all pairs of productPopularity1 into a list and print all of them. Repeats the same thing using productPopularity2.

*/

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
