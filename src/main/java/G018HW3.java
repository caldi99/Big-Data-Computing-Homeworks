import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.*;

public class G018HW3 {

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // MAIN PROGRAM
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: filepath k z L");
        }

        // ----- Initialize variables
        String filename = args[0];
        int k = Integer.parseInt(args[1]);
        int z = Integer.parseInt(args[2]);
        int L = Integer.parseInt(args[3]);
        long start, end; // variables for time measurements

        // ----- Set Spark Configuration
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true).setAppName("MR k-center with outliers");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // ----- Read points from file
        start = System.currentTimeMillis();
        JavaRDD<Vector> inputPoints = sc.textFile(args[0], L)
                .map(x -> strToVector(x))
                .repartition(L)
                .cache();
        long N = inputPoints.count();
        end = System.currentTimeMillis();

        // ----- Print input parameters
        System.out.println("File : " + filename);
        System.out.println("Number of points N = " + N);
        System.out.println("Number of centers k = " + k);
        System.out.println("Number of outliers z = " + z);
        System.out.println("Number of partitions L = " + L);
        System.out.println("Time to read from file: " + (end - start) + " ms");

        // ---- Solve the problem
        ArrayList<Vector> solution = MR_kCenterOutliers(inputPoints, k, z, L);

        // ---- Compute the value of the objective function
        start = System.currentTimeMillis();
        double objective = computeObjective(inputPoints, solution, z);
        end = System.currentTimeMillis();
        System.out.println("Objective function = " + objective);
        System.out.println("Time to compute objective function: " + (end - start) + " ms");

    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // AUXILIARY METHODS
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method strToVector: input reading
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method euclidean: distance function
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method MR_kCenterOutliers: MR algorithm for k-center with outliers
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Vector> MR_kCenterOutliers(JavaRDD<Vector> points, int k, int z, int L) {

        long start,end;
        //------------- ROUND 1 ---------------------------

        start=System.currentTimeMillis();
        JavaRDD<Tuple2<Vector, Long>> corset = points.mapPartitions(x ->
        {
            ArrayList<Vector> partition = new ArrayList<>();
            while (x.hasNext()) partition.add(x.next());
            ArrayList<Vector> centers = kCenterFFT(partition, k + z + 1);
            ArrayList<Long> weights = computeWeights(partition, centers);
            ArrayList<Tuple2<Vector, Long>> c_w = new ArrayList<>();
            for (int i = 0; i < centers.size(); ++i) {
                Tuple2<Vector, Long> entry = new Tuple2<>(centers.get(i), weights.get(i));
                c_w.add(i, entry);
            }
            return c_w.iterator();
        }); // END OF ROUND 1

        //------------- ROUND 2 ---------------------------

        ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>((k + z) * L);


        elems.addAll(corset.collect());
        System.out.println(elems);
        end = System.currentTimeMillis();
        System.out.println("Round 1 time : "+(end-start));

        //
        // ****** ADD YOUR CODE
        // ****** Compute the final solution (run SeqWeightedOutliers with alpha=2)
        // ****** Measure and print times taken by Round 1 and Round 2, separately
        // ****** Return the final solution
        //

        start = System.currentTimeMillis();
        ArrayList<Vector> P = new ArrayList<>((k + z) * L);
        ArrayList<Long> W = new ArrayList<>((k + z) * L);
        for (Tuple2<Vector, Long> elem : elems) {
            System.out.println(elem._1());
            System.out.println(elem._2());
            P.add(elem._1());
            W.add(elem._2());
        }
        initDistances(P);
        ArrayList<Vector> centers = SeqWeightedOutliers(P, W, k, z, 2);
        end = System.currentTimeMillis();
        System.out.println("Round 2 time : "+(end-start));
        return centers;

    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method kCenterFFT: Farthest-First Traversal
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Vector> kCenterFFT(ArrayList<Vector> points, int k) {

        final int n = points.size();
        double[] minDistances = new double[n];
        Arrays.fill(minDistances, Double.POSITIVE_INFINITY);

        ArrayList<Vector> centers = new ArrayList<>(k);

        Vector lastCenter = points.get(0);
        centers.add(lastCenter);
        double radius = 0;

        for (int iter = 1; iter < k; iter++) {
            int maxIdx = 0;
            double maxDist = 0;

            for (int i = 0; i < n; i++) {
                double d = euclidean(points.get(i), lastCenter);
                if (d < minDistances[i]) {
                    minDistances[i] = d;
                }

                if (minDistances[i] > maxDist) {
                    maxDist = minDistances[i];
                    maxIdx = i;
                }
            }

            lastCenter = points.get(maxIdx);
            centers.add(lastCenter);
        }
        return centers;
    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method computeWeights: compute weights of coreset points
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static ArrayList<Long> computeWeights(ArrayList<Vector> points, ArrayList<Vector> centers) {
        Long weights[] = new Long[centers.size()];
        Arrays.fill(weights, 0L);
        for (int i = 0; i < points.size(); ++i) {
            double tmp = euclidean(points.get(i), centers.get(0));
            int mycenter = 0;
            for (int j = 1; j < centers.size(); ++j) {
                if (euclidean(points.get(i), centers.get(j)) < tmp) {
                    mycenter = j;
                    tmp = euclidean(points.get(i), centers.get(j));
                }
            }
            // System.out.println("Point = " + points.get(i) + " Center = " + centers.get(mycenter));
            weights[mycenter] += 1L;
        }
        ArrayList<Long> fin_weights = new ArrayList<>(Arrays.asList(weights));
        return fin_weights;
    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method SeqWeightedOutliers: sequential k-center with outliers
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    static Double[][] distancesMatrix = null;

    //Method that computes the seqweightedoutliers
    public static ArrayList<Vector> SeqWeightedOutliers(ArrayList<Vector> P, ArrayList<Long> W, int k, int z, int alpha) {
        ArrayList<Vector> S = null;
        Vector[] Z = null;
        Long Wz = 0L;
        long max = 0L;
        Vector new_center = null;
        int guess = 1;
        double r = minDistance(k + z + 1) / 2;

        while (true) {
            S = new ArrayList<>();
            Z = new Vector[P.size()];
            P.toArray(Z);

            Wz = 0L;
            for (Long element : W)
                Wz += element;


            while ((S.size() < k) && (Wz > 0)) {
                max = 0L;
                new_center = null;
                for (Vector x : P) {
                    long ball_weigth = 0L;
                    ArrayList<Integer> vector1 = Bz(P, Z, x, (1 + 2 * alpha) * r); //index of where it is in P

                    for (Integer y : vector1)
                        ball_weigth += W.get(y);

                    if (ball_weigth > max) {
                        max = ball_weigth;
                        new_center = x;
                    }
                }
                S.add(new_center);
                ArrayList<Integer> vector = Bz(P, Z, new_center, (3 + 4 * alpha) * r);

                for (Integer index : vector) {
                    Z[index] = null;
                    Wz -= W.get(index);
                }
            }
            if (Wz <= z) {
                System.out.println("Final guess = : " + r);
                System.out.println("Number of guesses = " + guess);
                return S;
            } else {
                r *= 2;
                guess += 1;
            }
        }
    }

    //Computes the distance that is the minimum among the first numberOfPoints distances
    private static double minDistance(int numberOfPoints) {
        Double min = Double.MAX_VALUE;
        for (int i = 0; i < numberOfPoints; i++) {
            for (int j = i + 1; j < numberOfPoints; j++) {
                Double val = distancesMatrix[i][j];
                if (val < min)
                    min = val;
            }
        }
        return min;
    }

    //This function returns the indices of the elements of P that must be put inside Bz
    private static ArrayList<Integer> Bz(ArrayList<Vector> P, Vector[] Z, Vector x, double r) {
        ArrayList<Integer> Bz = new ArrayList<>();
        int indexX = P.indexOf(x);
        for (int i = 0; i < Z.length; i++)
            if ((distancesMatrix[indexX][i] <= r) && (Z[i] != null))
                Bz.add(i);
        return Bz;
    }

    //Method to initialize the distances
    public static void initDistances(ArrayList<Vector> initialPoints) {
        distancesMatrix = new Double[initialPoints.size()][initialPoints.size()];
        for (int i = 0; i < initialPoints.size(); i++) {
            distancesMatrix[i][i] = 0.0;
            for (int j = i + 1; j < initialPoints.size(); j++) {
                double d = Math.sqrt(Vectors.sqdist(initialPoints.get(i), initialPoints.get(j)));
                distancesMatrix[i][j] = d;
                distancesMatrix[j][i] = d;
            }
        }
    }


    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // Method computeObjective: computes objective function
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    public static double computeObjective(JavaRDD<Vector> points, ArrayList<Vector> centers, int z) {
        //
        // ****** ADD THE CODE FOR computeObjective
        //
        return points.mapToPair(point -> {
                    ArrayList<Double> distances = new ArrayList<>();
                    for (Vector center : centers) {
                        distances.add(euclidean(point, center));
                    }
                    distances.sort(Comparator.naturalOrder());
                    return new Tuple2<>(distances.get(0),0);
                }).sortByKey(false).collect().get(z)._1();
    }
}
