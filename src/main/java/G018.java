import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.IOException;
import java.util.ArrayList;

public class G018
{
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

        //RETRIEVE PARAMETERS
        int K = Integer.parseInt(args[0]);
        int H = Integer.parseInt(args[1]);
        String S = args[2];
        String path = args[3];

        //SPARK CONFIGURATIONS
        SparkConf conf = new SparkConf(true).setAppName("Homework1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //TASK 1 :
        JavaRDD<String> rawData = sc.textFile(path).repartition(K).cache();
        System.out.println("Number of rows : "+ rawData.count());

        //TASK 2 :
        JavaPairRDD<String,Integer> productCustomer;
        productCustomer = rawData.

                flatMapToPair(
                        (transaction)->
                        {
                            String[] elements = transaction.split(",");
                            Integer quantity = Integer.parseInt(elements[3]);
                            String city = elements[elements.length-1];
                            ArrayList<Tuple2<Tuple2<String,Integer>,Integer>> list = new ArrayList<>();
                            if(quantity > 0)
                            {
                                if(S.equals("all"))
                                    list.add(new Tuple2<>(new Tuple2<>(elements[1],Integer.parseInt(elements[6])), 0));
                                else if(city.equals(S))
                                    list.add(new Tuple2<>(new Tuple2<>(elements[1],Integer.parseInt(elements[6])), 0));
                            }
                            return list.iterator();
                        })

                .groupByKey()

                .mapToPair( (intermediatePair) -> new Tuple2<>(intermediatePair._1()._1(),intermediatePair._1()._2()) );

        System.out.println("Product-Customer Pairs = "+ productCustomer.count());

        //TASK 3
        JavaPairRDD<String,Integer> productPopularity1;

        productPopularity1 = productCustomer.

                mapPartitionsToPair((it) ->
                        {
                            ArrayList<Tuple2<String, Integer>> pairs = new ArrayList<>();
                            while(it.hasNext())
                            {
                                Tuple2<String,Integer> p = it.next();
                                pairs.add(new Tuple2<String,Integer>(p._1(),1));
                            }
                            return pairs.iterator();
                        }).

                groupByKey().

                mapValues((element)->
                        {
                            int sum =0;
                            for(int i : element)
                                sum += i;
                            return sum;
                        });

        //TASK 4

        JavaPairRDD<String,Integer> productPopularity2;
        productPopularity2 = productCustomer.

                mapToPair( (it) -> new Tuple2<>(it._1(),1) ).

                groupByKey().

                mapValues((element)->
                {
                    int sum =0;
                    for(int i : element)
                        sum += i;
                    return sum;
                });
    }
}

/*
PER MOSTRARE I RISULTATI
*  for (Tuple2<String,Integer> e : productPopularity2.collect())
            System.out.print("Product : " + e._1() + " Popularity : " + e._2() + " ");
*
Valutare se mettere al posto di mapvalues reduceByKey()
*/