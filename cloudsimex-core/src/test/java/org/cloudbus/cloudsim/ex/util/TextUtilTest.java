package org.cloudbus.cloudsim.ex.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.ex.util.TextUtil;
import org.cloudbus.cloudsim.ex.util.Textualize;
import org.junit.Test;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public class TextUtilTest {

    @Test
    public void testGetTxtLine() {
        String line = TextUtil.getTxtLine(new X(), ";", null, true);
        assertEquals(clean("LstInts=[...];LstStr=[...];X=      5;Y=6;Class=X"), clean(line));

        line = TextUtil.getTxtLine(new X(), ";", null, false);
        assertEquals(clean("  [...]; [...];      5;6;    X"), clean(line));

        line = TextUtil.getTxtLine(new ExtendsXNoAnno(), "|", null, true);
        assertEquals(clean("Prop=true|LstInts=[...]|LstStr=[...]|X=      5|Y=6|Class=ExtendsXNoAnno"), clean(line));

        line = TextUtil.getTxtLine(new ExtendsXNoAnno(), ";", null, false);
        assertEquals(clean("true;  [...]; [...];      5;6;ExtendsXNoAnno"), clean(line));

        line = TextUtil.getTxtLine(new ExtendsXAnno(), "|", null, true);
        assertEquals(clean("Prop=true|Y=6"), clean(line));

        line = TextUtil.getTxtLine(new ExtendsXAnno(), ";", null, false);
        assertEquals(clean("true;6"), clean(line));

        line = TextUtil.getTxtLine(new ExtendsXAnno(), ";", new String[] { "X", "Y" }, false);
        assertEquals(clean("5;6"), clean(line));

        line = TextUtil.getTxtLine(new ExtendsXAnno(), ";", new String[] { "Y", "X" }, false);
        assertEquals(clean("6;5"), clean(line));

        line = TextUtil.getTxtLine(new ExtendsXNoAnno(), ";", new String[] { "X" }, false);
        assertEquals(clean("5"), clean(line));
    }

    @Test
    public void testGetCaptionLine() {
        String line = TextUtil.getCaptionLine(X.class, ";");
        assertEquals(clean("LstInts;LstStr;      X;Y;Class"), clean(line));

        line = TextUtil.getCaptionLine(ExtendsXNoAnno.class, "|");
        assertEquals(clean("Prop|LstInts|LstStr|      X|Y|Class"), clean(line));

        line = TextUtil.getCaptionLine(ExtendsXAnno.class, "|");
        assertEquals(clean("Prop|Y"), clean(line));

        line = TextUtil.getCaptionLine(ExtendsXNoAnno.class, "|", new String[] { "X", "Prop" });
        assertEquals(clean("X|Prop"), clean(line));

        line = TextUtil.getCaptionLine(ExtendsXAnno.class, "|", new String[] { "X", "LstStr" });
        assertEquals(clean("X|LstStr"), clean(line));
    }

    private static String clean(final String line) {
        return line.replaceAll("\\s*", "");
    }

    private static class X {
        int x = 5;
        String y = "6";
        List<Integer> lstInts = Arrays.asList(5, 6);
        String[] lstStr = {};

        @SuppressWarnings("unused")
        public int getX() {
            return x;
        }

        @SuppressWarnings("unused")
        public String getY() {
            return y;
        }

        @SuppressWarnings("unused")
        public List<Integer> getLstInts() {
            return lstInts;
        }

        @SuppressWarnings("unused")
        public String[] getLstStr() {
            return lstStr;
        }

        @SuppressWarnings("unused")
        private String[] getPrivateStr() {
            return lstStr;
        }

        @SuppressWarnings("unused")
        private String[] getProtectedStr() {
            return lstStr;
        }
    }

    private static class ExtendsXNoAnno extends X {
        @SuppressWarnings("unused")
        public boolean isProp() {
            return true;
        }

        public int getX() {
            return x;
        }
    }

    @Textualize(properties = { "Prop", "Y" })
    private static class ExtendsXAnno extends X {
        @SuppressWarnings("unused")
        public boolean isProp() {
            return true;
        }
    }

}
