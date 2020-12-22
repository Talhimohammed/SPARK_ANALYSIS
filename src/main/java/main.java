import org.apache.hadoop.util.bloom.Key;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class main {
    public static void main(String[] args){

          new main().runWordCount("src/main/java/text.txt","outputcountword/output");
          new main().runTopicDetection("src/main/java/topicdetection/speech.txt","ouput/");
          new main().runSentimentAnalysis("src/main/java/sentimentanalysis/speech.txt","ouput/");

    }

    public void runWordCount(String inpupath ,String outpath){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Countword");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> scFile =sparkContext.textFile(inpupath);

        //Transformation
        JavaPairRDD<String,Integer> counts = scFile.flatMap(s -> Arrays.asList(s.split("\t")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        counts.saveAsTextFile(outpath);
    }

    public void runTopicDetection(String inputpath , String outputpath){

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Topicdetection");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> scFile =sparkContext.textFile(inputpath);
        JavaRDD<String> economic =sparkContext.textFile("src/main/java/topicdetection/economic.txt");
        JavaRDD<String> social =sparkContext.textFile("src/main/java/topicdetection/social.txt");
        JavaRDD<String> politic =sparkContext.textFile("src/main/java/topicdetection/politics.txt");
        List<String> ecoList = economic.collect();
        List<String> socialList = social.collect();
        List<String> politicList = politic.collect();
        HashMap<String,Integer> counter = new HashMap<String,Integer>();
        counter.put("economic",0);
        counter.put("social",0);
        counter.put("politic",0);

        List<Tuple2<String, Integer>> results =  scFile.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b )
                .collect();

        for (Tuple2<String,Integer> val : results){

              if( ecoList.contains(val._1)) {  counter.put("economic",counter.get("economic")+val._2);   }
              if (socialList.contains(val._1)){  counter.put("social",counter.get("social")+val._2); }
              if (politicList.contains(val._1)) {  counter.put("politic",counter.get("politic")+val._2); }
        }

        for (Map.Entry<String,Integer> entry : counter.entrySet()){
               System.out.println(entry);
        }



    }

    public void runSentimentAnalysis(String inputpath , String outpath ){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SentimentAnalysis");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> scFile =sparkContext.textFile(inputpath);
        JavaRDD<String> goodwords =sparkContext.textFile("src/main/java/sentimentanalysis/positive-words.txt");
        JavaRDD<String> negawords =sparkContext.textFile("src/main/java/sentimentanalysis/negative-words.txt");

        List<String> goodwordsList = goodwords.collect();
        List<String> badwordsList = negawords.collect();

        List<Tuple2<String, Integer>> results =  scFile.flatMap(s -> Arrays.asList(s.split("\\s+")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b )
                .collect() ;

        int countgoodwords = 0 ;
        int countbadwords = 0  ;

        for (Tuple2<String,Integer> val : results){
            if (goodwordsList.contains(val._1)){
                countgoodwords +=  val._2 ;
            }
            if (badwordsList.contains(val._1)){
                countbadwords +=  val._2 ;
            }
        }
        System.out.println( "Positive words :" + countgoodwords +" Negative words :"+ countbadwords );
        float sentiment = ((float)(countgoodwords - countbadwords) / (float) (countgoodwords + countbadwords));

        float perc = sentiment * 100 ;
        System.out.println("sentiement : "+perc+"%");

    }
}
