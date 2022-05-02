
import javafx.util.Pair;
import  org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.*;
import scala.Int;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static java.lang.Math.sqrt;

public class G018HW2 {

    public static Vector strToVector(String str) {
        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }


    public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }
        ArrayList<Vector> result = new ArrayList<>();
        Files.lines(Paths.get(filename))
                .map(str -> strToVector(str))
                .forEach(e -> result.add(e));
        return result;
    }



    static Double[][] distances = null;

    public static void main(String[] args) throws  IOException
    {
        ArrayList<Vector> inputPoints = readVectorsSeq(args[0]);

        distances = precomputeDistances(inputPoints);

        int k = Integer.parseInt(args[1]);
        int z = Integer.parseInt(args[2]);

        ArrayList<Long> weights  = new ArrayList<>(inputPoints.size());

        for(int i=0; i< inputPoints.size(); i++)
            weights.add(1L);

        Long startingTime = System.currentTimeMillis();

        ArrayList<Vector> solution =  SeqWeightedOutliers(inputPoints,weights,k,z,0);

        Long finishingTime = System.currentTimeMillis();

        double objective = ComputeObjective(inputPoints,solution,z);

        /* //TODO:
        * Return as output the following quantities: |P|, k, z, the initial guess made by SeqWeightedOutliers(inputPoints,weights,k,z,0), the value objective, and the time (in milliseconds)
        * required by the execution of SeqWeightedOutliers(inputPoints,weights,k,z,0). IT IS IMPORTANT THAT ALL PROGRAMS USE THE SAME OUTPUT FORMAT AS IN THE FOLLOWING EXAMPLE: link to be added
        * */

        System.out.println("Input size n = "+ inputPoints.size()); // |P|
        System.out.println("Number of centers k = " + k); //k
        System.out.println("Number of outliers z = " + z); //z
        System.out.println("Initial guess = "+ minDistance(inputPoints,k+z+1)/2); //initial guess ok
        System.out.println("Number of guesses = " ); //number of guesses?? sempre sbagliata
        System.out.println("Final guess = " ); //final guess??
        System.out.println("Objective function = "+ objective );
        System.out.println("Time of SeqWeightedOutliers = "+ (finishingTime-startingTime)); //time required in which format?? milliseconds??
    }


    public static Double[][] precomputeDistances(ArrayList<Vector> P)
    {
        Double[][] ret = new Double[P.size()][P.size()];
        for(int i=0; i< P.size(); i++)
        {
            ret[i][i] = 0.0;

        }

        for(int i=0; i<P.size(); i++)
        {
            for(int j=i+1; j<P.size(); j++)
            {
                ret[i][j] = Math.sqrt(Vectors.sqdist(P.get(i),P.get(j)));
                //if(i!= j) //better top rigth matrix

            }

        }
        for(int i=0; i<P.size(); i++)
        {
            for (int j = i + 1; j < P.size(); j++)
            {
                ret[j][i] = ret[i][j];
            }
        }


        return ret;




    }


    /**
     * z ?? type ??
     * k ?? type ??
     * alpha ?? type??
     * */
    public static  ArrayList<Vector> SeqWeightedOutliers(ArrayList<Vector> P, ArrayList<Long> W, int k, int z, int alpha )
    {
        //Variable declarations
        ArrayList<Vector> S = null;
        ArrayList<Vector> Z = null;
        Long Wz = 0L;
        Long max = 0L;
        Vector new_center = null;


        double r = minDistance(P,k+z+1)/2;
        int guess = 1;


        // Need to precompute the distances






        while (true)
        {
            S = new ArrayList<>(); // S = empty
            Z = new ArrayList<>(P); // Z = P

            Wz = 0L;
            for (Long element: W) //Wz = sun x \in P (w(x))
                Wz += element;

            while ((S.size() < k) && (Wz > 0))
            {
                max = 0L;
                new_center = null;
                for (Vector x: P)
                {
                    Long ball_weigth = 0L;
                    ArrayList<Vector> vector1 = Bz(Z,x,(1 + 2 * alpha)*r,P);

                    for (Vector y : vector1)
                        ball_weigth += W.get(P.indexOf(y));
                    if(ball_weigth > max)
                    {
                        max = ball_weigth;
                        new_center = x;
                    }
                }

                S.add(new_center);

                ArrayList<Vector> vector = Bz(Z,new_center,(3+ 4 * alpha)*r,P);
                for (Vector y: vector)
                {
                    Z.remove(y);
                    Wz-= W.get(P.indexOf(y));
                }
            }
            System.out.println("Iterazione : "+guess);
            if(Wz <= z)
            {
                System.out.println("R : "+r);
                System.out.println("GUESS : "+guess);
                return S;

            }
            else
            {
                r *= 2;
                guess += 1;
            }
        }
    }

    private static double minDistance(ArrayList<Vector> points, long numberOfPoints )
    {
        double min = Double.MAX_VALUE;
        for (int i =0; i<numberOfPoints; i++)
        {
            for (int j =i+1; j<numberOfPoints; j++)
            {
                double val = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                if(val < min)
                    min = val;
            }
        }
        return min;
    }

    private static ArrayList<Vector> Bz(ArrayList<Vector> Z,Vector x, double r,ArrayList<Vector> P)
    {
        ArrayList<Vector> Bz = new ArrayList<>();
        /*for (Vector y:Z)
        {
            if(Math.sqrt(Vectors.sqdist(x, y)) <= r)
                Bz.add(y);
        }*/

        for (Vector y: P )
        {
            if(distances[P.indexOf(x)][P.indexOf(y)] <= r)
            {
                Bz.add(y);
            }
        }


        return Bz;
    }

    public static double ComputeObjective(ArrayList<Vector> P, ArrayList<Vector> S, int z)
    {
        double[] distances = new double[P.size()];

        //Compute distance between points and centers
        for(int i = 0; i < distances.length; i++){
            distances[i] = computeDistance(P.get(i),S);
        }
        Arrays.sort(distances);


        //If we want to return the largest distance excluding z

        return distances[distances.length - z - 1];

        //Return the sum of all distances excluding Z set :

        /*
        double sum_distances = 0;
        for(int i = 0; i < distances.length - z; i++)
            sum_distances += distances[i];
        return sum_distances;
        */

        //This return must be eliminated, but before we must decide which return we want to use
        //return 0;
    }

    private static double computeDistance(Vector point, ArrayList<Vector> S){

        double min_distance = -1;
        double actual_distance = 0;

        //Computer the distance between the point and every center
        for(Vector center : S){
            actual_distance = Math.sqrt(Vectors.sqdist(point, center));

            if(actual_distance < min_distance || min_distance == -1){
                min_distance = actual_distance;
            }
        }
        //Return the distance between the point and the closer center
        return min_distance;
    }
}
