package sources.main;

public class SourceUtils {

    public static int computeTaskId(String one, String two) {
	String c = one.concat(two);
	return c.hashCode();
    }
}
