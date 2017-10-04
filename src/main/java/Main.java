import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple1;
import scala.Tuple2;

public class Main {
	public static void main(String[] args) {
		//count of the words in SPARK/data/data.txt
		Test1();
		//count of the most popular words in SPARK/data/data.txt
		Test2();
	}
	
	public static void Test1() {
		JavaSparkContext sc = sc();
		JavaRDD<String> rdd = sc.textFile("e:/work/projects/Eclipce/SPARK/data/*.txt");
		JavaRDD<Integer> res = rdd.map(String::length);
		int len = res.reduce((a, b) -> a + b);
		System.out.println("out : " + len);
		sc.close();
	}
	
	
	public static void Test2() {
		JavaSparkContext sc = sc();
		JavaRDD<String> rdd = sc.textFile("e:/work/projects/Eclipce/SPARK/data/*.txt");
		System.out.println("out : " + topX(rdd, 3));
		sc.close();
	}
	
	public static JavaSparkContext sc() {
		SparkConf c = new SparkConf();
		c.setAppName("my app");
		c.setMaster("local[1]");
		JavaSparkContext sc = new JavaSparkContext(c); 
		return sc;
	}
	
	public static List<String> topX(JavaRDD<String> lines, int x) {
		return lines.map(String::toLowerCase)
				
				.flatMap(WordsUtil::getWords)
				
				.mapToPair(word-> new Tuple2<>(word, 1))
				.reduceByKey((a, b)->a+b)
				.mapToPair(Tuple2::swap)
				.sortByKey(false)
				
				.map(v-> v._2())
				
				.take(x);
    }
}