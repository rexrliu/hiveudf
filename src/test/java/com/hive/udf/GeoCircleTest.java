package com.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GeoCircleTest {
    static final String res = new String("POLYGON ((108.913652 34.24387632059228, 108.91346308745578 34.24386265789172, 108.91327991501329 34.24382208492428, 108.91310804835342 34.24375583447906, 108.91295270961612 34.243665919542295, 108.9128186187215 34.24355507213385, 108.91270984995354 34.243426660296144, 108.91262970816506 34.24328458575786, 108.91258062836519 34.24313316538199, 108.91256410173999 34.242977, 108.9125806303529 34.242820834618016, 108.91262971190075 34.242669414242144, 108.91270985498663 34.24252733970387, 108.91281862444492 34.242398927866155, 108.91295271533954 34.24228808045771, 108.91310805338651 34.242198165520946, 108.913279918749 34.242131915075724, 108.9134630894435 34.24209134210829, 108.913652 34.242077679407736, 108.91384091055649 34.24209134210829, 108.91402408125099 34.242131915075724, 108.91419594661349 34.242198165520946, 108.91435128466046 34.24228808045771, 108.91448537555507 34.242398927866155, 108.91459414501337 34.24252733970387, 108.91467428809926 34.242669414242144, 108.91472336964708 34.242820834618016, 108.91473989826 34.242977, 108.91472337163479 34.24313316538199, 108.91467429183494 34.24328458575786, 108.91459415004645 34.243426660296144, 108.91448538127847 34.24355507213385, 108.91435129038386 34.243665919542295, 108.91419595164658 34.24375583447906, 108.91402408498669 34.24382208492428, 108.91384091254422 34.24386265789172, 108.913652 34.24387632059228))");

    static final double lat = 34.242977;
    static final double lng = 108.913652;
    static final double radius = 100;
    static final int sides = 36;

    GeoCircle udf;

    @Before
    public void setUp() throws Exception {
        udf = new GeoCircle();
    }

    @Test
    public void evaluate() throws HiveException {
        String o_lat = String.valueOf(lat);
        String o_lng = String.valueOf(lng);
        String o_radius = String.valueOf(radius);
        String o_sides = String.valueOf(sides);
        assertEquals(res, udf.evaluate(o_lat, o_lng, o_radius, o_sides));
    }
}