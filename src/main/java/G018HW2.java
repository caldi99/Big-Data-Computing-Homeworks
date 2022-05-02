
import  org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

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


    public static void main(String[] args) throws  IOException
    {
        ArrayList<Vector> inputPoints = readVectorsSeq(args[0]);
        int k = Integer.parseInt(args[1]);
        int z = Integer.parseInt(args[2]);

        ArrayList<Long> weights  = new ArrayList<>(inputPoints.size());

        for(int i=0; i< inputPoints.size(); i++)
            weights.add(1L);

        long startTime = System.nanoTime();
        ArrayList<Vector> solution =  SeqWeightedOutliers(inputPoints,weights,k,z,0);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime) / 1000000;
        double objective = ComputeObjective(inputPoints,solution,z);

        /* //TODO:
        * Return as output the following quantities: |P|, k, z, the initial guess made by SeqWeightedOutliers(inputPoints,weights,k,z,0), the value objective, and the time (in milliseconds)
        * required by the execution of SeqWeightedOutliers(inputPoints,weights,k,z,0). IT IS IMPORTANT THAT ALL PROGRAMS USE THE SAME OUTPUT FORMAT AS IN THE FOLLOWING EXAMPLE: link to be added
        * */
        System.out.print("Input size n = " + inputPoints.size() + "\n");
        System.out.print("Number of centers k = " + k + "\n");
        System.out.print("Number of outliers z = " + z + "\n");
        //System.out.print("Initial guess = " + z + "\n");
        //System.out.print("Final guess = " + z + "\n");
        //System.out.print("Number of guesses = " + z + "\n");
        System.out.print("Objective function = " + objective + "\n");
        System.out.print("Time of SeqWeightedOutliers = " + duration + "\n");

    }


    /**
     * z ?? type ??
     * k ?? type ??
     * alpha ?? type??
     * */
    public static  ArrayList<Vector> SeqWeightedOutliers(ArrayList<Vector> P, ArrayList<Long> W, int k, int z, int alpha )
    {
        ArrayList<Vector> S = null;
        double r = minDistance(P,k+z+1)/2;  // nella precedente versione non c'era il fratto 2
        System.out.print("Initial guess = " + r + "\n");
        int itera = 1;
        while (true)
        {
            S = new ArrayList<>(); // S = empty
            ArrayList<Vector> Z = new ArrayList<>(P); // Z = P
            Long Wz = 0L;
            for (Long element: W)
                Wz += element;

            while ((S.size() < k) && (Wz > 0))
            {
                long max = 0L;
                Vector new_center = null;
                for (Vector x: P)
                {
                    Long ball_weigth = 0L;
                    for (Vector y:Bz(Z,x,(1+2*alpha)))  // (1+2*alpha)*r ?? r dove viene fuori sulle slide??
                        ball_weigth += W.get(P.indexOf(y));
                    if(ball_weigth > max)
                    {
                        max = ball_weigth;
                        new_center = x;
                    }
                }
                S.add(new_center);
                for (Vector y:Bz(Z,new_center,(3+4*alpha)))
                {
                    Z.remove(y);
                    Wz-= W.get(P.indexOf(y));
                }
            }
            if(Wz <= z) {
                return S;
            }
            else {
                r *= 2;
                itera = itera + 1;
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
                double val = sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                if(val < min)
                    min = val;
            }
        }
        return min;
    }

    private static ArrayList<Vector> Bz(ArrayList<Vector> Z,Vector x, double r)
    {
        ArrayList<Vector> Bz = new ArrayList<>();
        for (Vector y:Z)
        {
            if(sqrt(Vectors.sqdist(x, y)) <= r)
                Bz.add(y);
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
