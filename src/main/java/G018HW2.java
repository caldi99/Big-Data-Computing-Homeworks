
import  org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import static java.lang.Math.sqrt;

public class G018HW2 {


    /**
     * z ?? type ??
     * k ?? type ??
     * alpha ?? type??
     * */
    public static  ArrayList<Vector> SeqWeightedOutliers(ArrayList<Vector> P, ArrayList<Long> W, int k, int z, int alpha )
    {
        ArrayList<Vector> S = null;
        double r = minDistance(P,k+z+1);

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
            if(Wz <= z)
                return S;
            else
                r*= 2;
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

    public double ComputeObjective(ArrayList<Vector> P, ArrayList<Vector> S, int z)
    {
        double[] distances = new double[P.size()];

        //Compute distance between points and centers
        for(int i = 0; i < distances.length; i++){
            distances[i] = computeDistance(P.get(i),S);
        }
        Arrays.sort(distances);

        //If we want to return the largest distance excluding z

        //return distances[distances.length - z - 1];

        //Return the sum of all distances excluding Z set :

        /*
        double sum_distances = 0;
        for(int i = 0; i < distances.length - z; i++)
            sum_distances += distances[i];
        return sum_distances;
        */

        //This return must be eliminated, but before we must decide which return we want to use
        return 0;
    }

    private double computeDistance(Vector point, ArrayList<Vector> S){

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
