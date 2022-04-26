
import  org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.*;

import java.util.ArrayList;

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
                double val = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
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
            if(Math.sqrt(Vectors.sqdist(x, y)) <= r)
                Bz.add(y);
        }
        return Bz;
    }


}
