package vincent.kiseki.dbutils.utils;

public class TypeUtils {

    public static String attributeToUp(String attr){
        return "up" + ("" + attr.charAt(0)).toUpperCase()
            + attr.substring(1);
    }

    public static String attributeToDown(String attr){
        return "dn" + ("" + attr.charAt(0)).toUpperCase()
            + attr.substring(1);
    }
}
