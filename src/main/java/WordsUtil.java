import java.util.Arrays;
import java.util.Iterator;

public class WordsUtil {
	public static Iterator<String> getWords(String s) {  
		return Arrays.asList(s.split(" ")).iterator();
	}
}