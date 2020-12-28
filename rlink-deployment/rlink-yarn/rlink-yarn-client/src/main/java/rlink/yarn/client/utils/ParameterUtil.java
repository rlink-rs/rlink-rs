package rlink.yarn.client.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ParameterUtil {

    public static Map<String, String> fromArgs(String[] args) {
        Map<String, String> map = new HashMap<>(args.length / 2);
        int i = 0;
        while (i < args.length) {
            String key;
            if (args[i].startsWith("--")) {
                key = args[i].substring(2);
            } else if (args[i].startsWith("-")) {
                key = args[i].substring(1);
            } else {
                throw new IllegalArgumentException(
                        String.format("Error parsing arguments '%s' on '%s'. Please prefix keys with -- or -.",
                                Arrays.toString(args), args[i]));
            }
            if (key.isEmpty()) {
                throw new IllegalArgumentException(
                        "The input " + Arrays.toString(args) + " contains an empty argument");
            }

            i++;
            if (i >= args.length) {
                continue;
            }
            if (args[i].startsWith("--") || args[i].startsWith("-")) {
                continue;
            }
            map.put(key, args[i]);
            i++;
        }
        return map;
    }

}
