package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.stringtemplate.v4.ST;

public class GeoCircle extends UDF {
    // WGS-84 ellipsoid, radius of sphere of equal area
    private static final double EQUAL_AREA_RADIUS = 6371007.1809;

    private static double hav(double x) {
        return Math.pow(Math.sin(x / 2), 2);
    }

    public String evaluate(String _lat, String _lng, String _radius, String _sides) throws HiveException {
        double lat = Double.parseDouble(_lat);
        double lng = Double.parseDouble(_lng);
        double r = Double.parseDouble(_radius);
        int n = Integer.parseInt(_sides);

        if (r <= 0) {
            throw new HiveException(String.valueOf(r) + " radius must be positive.");
        }

        if (n <= 0) {
            throw new HiveException(String.valueOf(r) + " number of sides must be an positive integer.");
        }

        if (n % 2 != 0) {
            throw new HiveException(String.valueOf(n) + " number of sides must be even.");
        }

        // use haversine formula for to reduce numerical error
        double a = r / EQUAL_AREA_RADIUS;  // angle corresponding to the radius (in earth arc length)
        double hav_a = hav(a);

        double cx = Math.toRadians(lat);  // latitude in radian
        double cy = Math.toRadians(lng);  // longitude in radian
        double cos_c = Math.cos(cx);  // cos of latitude

        double delta = 2 * Math.PI / n;  // step size for latitude

        double[][] v = new double[n][2];

        // start point on latitude line
        v[0][0] = Math.toDegrees(cx + a);
        v[0][1] = lng;

        // end point on latitude line
        v[n / 2][0] = Math.toDegrees(cx - a);
        v[n / 2][1] = lng;

        double x = 0;
        double y = 0;

        for (int i = 1; i < n / 2; i++) {
            // cos step size on latitude is used to ensure that
            // the generate points are equally distributed on the circle
            x = cx + a * Math.cos(i * delta);
            v[i][0] = Math.toDegrees(x);
            v[n - i][0] = v[i][0];  // WKT order

            y = Math.sqrt((hav_a - hav(cx - x)) / (cos_c * Math.cos(x)));

            if (y > 1) {  // y as a sin should be within [-1, 1]
                y = 1;
            }
            if (y < -1) {  // y as a sin should be within [-1, 1]
                y = -1;
            }

            y = 2 * Math.asin(y);
            v[i][1] = Math.toDegrees(cy - y);  // WKT order
            v[n - i][1] = Math.toDegrees(cy + y);
        }

        // represent the generated circle as a WKT polygon
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("POLYGON ((");
        for (int i = 0; i < n; i++) {
            stringBuilder.append(v[i][1]);
            stringBuilder.append(" ");
            stringBuilder.append(v[i][0]);
            stringBuilder.append(", ");
        }
        // end with start point
        stringBuilder.append(v[0][1]);
        stringBuilder.append(" ");
        stringBuilder.append(v[0][0]);
        stringBuilder.append("))");

        return stringBuilder.toString();
    }
}
