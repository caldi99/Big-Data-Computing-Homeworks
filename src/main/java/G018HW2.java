
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

    static Double[][] distancesMatrix = null;

    public static void main(String[] args) throws  IOException
    {
        ArrayList<Vector> inputPoints = readVectorsSeq(args[0]);
        int k = Integer.parseInt(args[1]);
        int z = Integer.parseInt(args[2]);
        ArrayList<Long> weights  = new ArrayList<>(inputPoints.size());
        for(int i=0; i< inputPoints.size(); i++)
            weights.add(1L);

        initDistances(inputPoints);

        System.out.println("Input size n = "+ inputPoints.size());
        System.out.println("Number of centers k = " + k);
        System.out.println("Number of outliers z = " + z);
        System.out.println("Initial guess = "+ minDistance(inputPoints,k+z+1)/2);

        Long startingTime = System.currentTimeMillis();
        ArrayList<Vector> solution =  SeqWeightedOutliers(inputPoints,weights,k,z,0);
        Long finishingTime = System.currentTimeMillis();

        double objective = ComputeObjective(inputPoints,solution,z);
        System.out.println("Objective function = " + objective );
        System.out.println("Time of SeqWeightedOutliers = "+ (finishingTime-startingTime)); //time required in which format?? milliseconds??
    }

    public static void initDistances(ArrayList<Vector> initialPoints)
    {
        distancesMatrix = new Double[initialPoints.size()][initialPoints.size()];
        for(int i=0; i<initialPoints.size(); i++)
        {
            distancesMatrix[i][i]= 0.0;
            for(int j=i+1; j< initialPoints.size(); j++)
            {
                double d = Math.sqrt(Vectors.sqdist(initialPoints.get(i),initialPoints.get(j)));
                distancesMatrix[i][j] = d;
                distancesMatrix[j][i] = d;
            }
        }
    }

    public static  ArrayList<Vector> SeqWeightedOutliers(ArrayList<Vector> P, ArrayList<Long> W, int k, int z, int alpha )
    {
        ArrayList<Vector> S = null;
        Vector[] Z = null;
        Long Wz = 0L;
        long max = 0L;
        Vector new_center = null;
        int guess = 1;
        double r = minDistance(P,k+z+1)/2;

        while (true)
        {
            S = new ArrayList<>();
            Z = new Vector[P.size()];
            P.toArray(Z);

            Wz = 0L;
            for (Long element: W) //Wz = sun x \in P (w(x))
                Wz += element;

            //Wz += W.size();

            while ((S.size() < k) && (Wz > 0))
            {
                max = 0L;
                new_center = null;
                for (Vector x: P)
                {
                    long ball_weigth = 0L;
                    ArrayList<Integer> vector1 = Bz(P,Z,x,(1 + 2 * alpha)*r); //index of where it is in P

                    for (Integer y : vector1)
                       ball_weigth += W.get(y);

                    //ball_weigth += vector1.size();

                    if(ball_weigth > max)
                    {
                        max = ball_weigth;
                        new_center = x;
                    }
                }
                S.add(new_center);
                ArrayList<Integer> vector = Bz(P,Z,new_center,(3+ 4 * alpha) * r);

                for (Integer index: vector)
                {
                    Z[index] = null;
                    Wz -= W.get(index);
                }
            }
            if(Wz <= z)
            {
                System.out.println("Final guess = : "+r);
                System.out.println("Number of guesses = "+guess);
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
        Double min = Double.MAX_VALUE;
        for (int i =0; i<numberOfPoints; i++)
        {
            for (int j =i+1; j<numberOfPoints; j++)
            {
                Double val = distancesMatrix[i][j];
                if(val < min)
                    min = val;
            }
        }
        return min;
    }

    private static ArrayList<Integer> Bz(ArrayList<Vector> P,Vector[] Z ,Vector x, double r)
    {
        ArrayList<Integer> Bz = new ArrayList<>();
        int indexX = P.indexOf(x);
        for(int i=0; i< Z.length; i++)
            if((distancesMatrix[indexX][i] <= r) && (Z[i] != null))
                Bz.add(i);
        return Bz;
    }

    public static double ComputeObjective(ArrayList<Vector> P, ArrayList<Vector> S, int z)
    {
        double[] distances = new double[P.size()];
        for(int i = 0; i < distances.length; i++){
            distances[i] = computeDistance(P.get(i),S);
        }
        Arrays.sort(distances);
        return distances[distances.length - z - 1];
    }

    private static double computeDistance(Vector point, ArrayList<Vector> S){

        double min_distance = -1;
        double actual_distance = 0;

        for(Vector center : S){
            actual_distance = Math.sqrt(Vectors.sqdist(point, center));

            if(actual_distance < min_distance || min_distance == -1){
                min_distance = actual_distance;
            }
        }
        return min_distance;
    }
}
