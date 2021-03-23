import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.functions;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;
import java.util.regex.Pattern;
import java.util.*;
import java.io.*;
import java.lang.Math.*;


public class BatchProcessingApplication_1702281 {
	
	//declare REGEX values
	private static final Pattern LETTERS = Pattern.compile("[^a-z]+");
	private static final Pattern WORDS = Pattern.compile("[\\,\\.\\-\\{\\}\\(\\)\\[\\]\\\"\\;\\:\\!\\?\\_\t]+| +");
	private static final String LOWERCASE = "^[a-z]+$";
	private static boolean amazonEMR;
	

	public static void main(String[] args) {
	
		//start application
		System.out.println("APPLICATION START");
		System.out.println("--------------------------");
		
		String bucketName = null;
	        String bucketItemKey = null;
	        AmazonS3 client = null;
	        String fileName = "";
		
		//determine if the program is being run on EMR based on args
		if(args.length == 0) {
			amazonEMR = false;
		} else {
			bucketName = args[0];
	        	bucketItemKey = args[1];
	        	client = AmazonS3ClientBuilder.standard().build();
	        	amazonEMR = true;
	        }		
		
		//declare Spark variables
		SparkConf conf = new SparkConf().setAppName("Batch Processing Application").set("spark.executor.memory","2g");
		JavaSparkContext context = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("Batch Processing Application").getOrCreate();
		
		JavaRDD<String> textLines = null;
		
		//declare file search variables if running on local cluster
		if(amazonEMR == false) {
			Scanner scan = new Scanner(System.in);
			fileName = "";
		
			boolean invalid = false;
			//get file name from user (loop until valid file found)
			do {
				//set invalid to true
			 	invalid = false;
			
			//user enters file
				System.out.print("Please enter .txt file name (Must be in same directory, do not include the .txt extention): ");
				fileName = scan.nextLine();
			
				fileName = fileName + ".txt";
			
				System.out.println("Searching for " + fileName);
				
				//try to get file and get each line from the text file
				try{
					textLines = spark.read().textFile(fileName).javaRDD();
				} catch(Exception e) {
					//show error message and set invalid to true if file not found
					System.out.println("Cannot find file, please make sure your file is in the same directory and does not include the .txt extention");
					invalid = true;
				}
			
			}while(invalid);
			
		} else {
			
			//try to get file on S3 Bucket and get each line from the text file
			try{
				fileName = "s3a://"+bucketName+"/"+bucketItemKey;
				textLines = spark.read().textFile(fileName).javaRDD();
			} catch(Exception e) {
				//kill application because bucket object can't be found
				System.exit(0);
			}
		}
			
		
		//break each line into words, seperating using punctuation and spaces
		JavaRDD<String> textWords = textLines.flatMap(s -> Arrays.asList(WORDS.split(s.toLowerCase())).iterator());
		
		//remove any invalid words / null entries
		textWords = textWords.filter(s -> s.matches(LOWERCASE));
		
		//create a PairRDD structure with the count of each word
		JavaPairRDD<String,Integer> wordCount = textWords.mapToPair(s -> new Tuple2<>(s,1));
		wordCount = wordCount.reduceByKey((s1,s2) -> s1 + s2);
		wordCount = wordCount.sortByKey(false);
		
		//break each word into letters
		JavaRDD<String> textLetters = textLines.flatMap(s -> Arrays.asList(LETTERS.split(s.toLowerCase())).iterator());
		textLetters = textLetters.filter(s -> s.matches(LOWERCASE));
		textLetters = textLetters.flatMap(s -> Arrays.asList(s.toLowerCase().split("")).iterator());
		
		//create a PairRDD structure with the count of each letter
		JavaPairRDD<String,Integer> letterCount = textLetters.mapToPair(s -> new Tuple2<>(s,1));
		letterCount = letterCount.reduceByKey((s1,s2) -> s1 + s2);
		letterCount = letterCount.sortByKey(false);
		
		//create structure to sort by the count
		JavaPairRDD<Integer,String> wordSwap = wordCount.mapToPair(t -> new Tuple2<Integer,String>(t._2(),t._1()));
		wordSwap = wordSwap.sortByKey(true);
		JavaPairRDD<Integer,String> letterSwap = letterCount.mapToPair(t -> new Tuple2<Integer,String>(t._2(),t._1()));
		letterSwap = letterSwap.sortByKey(true);
		
		//pass altered structure to original structure
		wordCount = wordSwap.mapToPair(t -> new Tuple2<String,Integer>(t._2(),t._1()));
		letterCount = letterSwap.mapToPair(t -> new Tuple2<String,Integer>(t._2(),t._1()));
		
		
		//create a RDD of ranks for each letter
		List tempWordRank = new ArrayList();
		int wordLength = (int)wordCount.count();
		for(int i = 0; i < wordLength; i++) {
			tempWordRank.add(wordLength-i);
		}
		
		List tempLetterRank = new ArrayList();
		int letterLength = (int)letterCount.count();
		for(int i = 0; i < letterLength; i++) {
			tempLetterRank.add(letterLength-i);
		}
		
		JavaRDD<Integer> wordRank = context.parallelize(tempWordRank);
		JavaRDD<Integer> letterRank = context.parallelize(tempLetterRank);

		//create structures for DataFrame
		StructType structureWordCount = DataTypes.createStructType(new StructField[]{
			DataTypes.createStructField("Word", DataTypes.StringType, true),
			DataTypes.createStructField("Frequency", DataTypes.IntegerType,true),
		});
		
		StructType structureLetterCount = DataTypes.createStructType(new StructField[]{
			DataTypes.createStructField("Letter", DataTypes.StringType, true),
			DataTypes.createStructField("Frequency", DataTypes.IntegerType,true),
		});
			
		StructType structureRank = DataTypes.createStructType(new StructField[]{
			DataTypes.createStructField("Rank", DataTypes.IntegerType,true),
		});
				
		//create rows for DataFrame
		JavaRDD<Row> dataRowsWordCount = wordCount.map(s -> RowFactory.create(s._1(),s._2()));
		JavaRDD<Row> dataRowsWordRank = wordRank.map(s -> RowFactory.create(s));
		
		JavaRDD<Row> dataRowsLetterCount = letterCount.map(s -> RowFactory.create(s._1(),s._2()));
		JavaRDD<Row> dataRowsLetterRank = letterRank.map(s -> RowFactory.create(s));
		
		//create data structures
		Dataset<Row> dataSetWordCount= spark.createDataFrame(dataRowsWordCount,structureWordCount);
		Dataset<Row> dataSetWordRank = spark.createDataFrame(dataRowsWordRank,structureRank);
			
		Dataset<Row> dataSetLetterCount= spark.createDataFrame(dataRowsLetterCount,structureLetterCount);
		Dataset<Row> dataSetLetterRank = spark.createDataFrame(dataRowsLetterRank,structureRank);
		
		//set name for ID values
		String countID = "idCount";
		String rankID = "idRank";
		
		//creating matching IDs for both datasets
		dataSetWordCount = dataSetWordCount.coalesce(1);
		dataSetWordCount = dataSetWordCount.withColumn(countID, functions.monotonically_increasing_id());
		dataSetWordRank = dataSetWordRank.coalesce(1);
		dataSetWordRank = dataSetWordRank.withColumn(rankID, functions.monotonically_increasing_id());
		
		dataSetLetterCount = dataSetLetterCount.coalesce(1);
		dataSetLetterCount = dataSetLetterCount.withColumn(countID, functions.monotonically_increasing_id());
		dataSetLetterRank = dataSetLetterRank.coalesce(1);
		dataSetLetterRank = dataSetLetterRank.withColumn(rankID, functions.monotonically_increasing_id());
		
		//merge the two datasets using IDs
		Dataset<Row> wordResult = dataSetWordRank.join(dataSetWordCount, dataSetWordCount.col(countID).equalTo(dataSetWordRank.col(rankID)));
		wordResult = wordResult.drop(countID).drop(rankID).sort(functions.asc("Rank"));
		
		Dataset<Row> letterResult = dataSetLetterRank.join(dataSetLetterCount, dataSetLetterCount.col(countID).equalTo(dataSetLetterRank.col(rankID)));
		letterResult = letterResult.drop(countID).drop(rankID).sort(functions.asc("Rank"));
		
		//get sum of all words
		Long wordSum = wordResult.agg(functions.sum(wordResult.col("Frequency"))).first().getLong(0);
		
		//set thresholds for words
		String popularWordThreshold = String.valueOf(Math.ceil(wordLength * 0.05));
		String commonLowerWordThreshold = String.valueOf(Math.floor(wordLength * 0.475));
		String commonUpperWordThreshold = String.valueOf(Math.ceil(wordLength * 0.525));
		String rareWordThreshold = String.valueOf(Math.floor(wordLength * 0.95));
		
		//set word filters
		String popularWordsFilter = "Rank >= 0 and Rank <= " + popularWordThreshold.replaceAll("0+$","").replaceAll("\\.$","");
		String commonWordsFilter = "Rank >= " + commonLowerWordThreshold.replaceAll("0+$","").replaceAll("\\.$","") + " and Rank <=" + commonUpperWordThreshold.replaceAll("0+$","").replaceAll("\\.$","");
		String rareWordsFilter = "Rank >= " + rareWordThreshold.replaceAll("0+$","").replaceAll("\\.$","") + " and Rank <= " + String.valueOf(wordLength);
		
		//create word categories using filters
		Dataset<Row> popularWords = wordResult.filter(popularWordsFilter);
		Dataset<Row> commonWords = wordResult.filter(commonWordsFilter);
		Dataset<Row> rareWords = wordResult.filter(rareWordsFilter);
		
		//get the range of each category
		int popularWordsRange = (int)(Math.ceil(wordLength * 0.05));
		int commonWordsRange = (int)(1 + ((Math.ceil(wordLength * 0.525)) - (Math.floor(wordLength * 0.475))));
		int rareWordsRange = (int)(1 + (wordLength - Math.floor(wordLength * 0.95)));
		
		//set thresholds for letters
		String popularLetterThreshold = String.valueOf(Math.ceil(letterLength * 0.05));
		String commonLowerLetterThreshold = String.valueOf(Math.floor(letterLength * 0.475));
		String commonUpperLetterThreshold = String.valueOf(Math.ceil(letterLength * 0.525));
		String rareLetterThreshold = String.valueOf(Math.floor(letterLength * 0.95));
		
		//set letter filters
		String popularLettersFilter = "Rank >= 0 and Rank <= " + popularLetterThreshold.replaceAll("0+$","").replaceAll("\\.$","");
		String commonLettersFilter = "Rank >= " + commonLowerLetterThreshold.replaceAll("0+$","").replaceAll("\\.$","") + " and Rank <=" + commonUpperLetterThreshold.replaceAll("0+$","").replaceAll("\\.$","");
		String rareLettersFilter = "Rank >= " + rareLetterThreshold.replaceAll("0+$","").replaceAll("\\.$","") + " and Rank <= " + String.valueOf(wordLength);
		
		//create letter categories using filters
		Dataset<Row> popularLetters = letterResult.filter(popularLettersFilter);
		Dataset<Row> commonLetters = letterResult.filter(commonLettersFilter);
		Dataset<Row> rareLetters = letterResult.filter(rareLettersFilter);
		
		//get the range of each category
		int popularLettersRange = (int)(Math.ceil(letterLength * 0.05));
		int commonLettersRange = (int)(1 + ((Math.ceil(letterLength * 0.525)) - (Math.floor(letterLength * 0.475))));
		int rareLettersRange = (int)(1 + (letterLength - Math.floor(letterLength * 0.95)));
		
		//set line breaks 
		String lineBreak = "\n---------------------------------------------------------------------------------------------\n";
		String lineBreakStart = "---------------------------------------------------------------------------------------------\n";
		String lineBreakEnd = "\n---------------------------------------------------------------------------------------------";
		
		//create the text file as a String 
		String outputString = lineBreakStart + "Output for " + fileName + lineBreak + "('total number of words=', " + wordSum + ")\n('total number of distinct words=', " + wordLength + ")\n('popular_threshold_word=', " + popularWordThreshold + ")\n('common_threshold_l_word=', " + commonLowerWordThreshold + ")\n('common_threshold_u_word=', " + commonUpperWordThreshold + ")\n('rare_threshold_word=', " + rareWordThreshold + ")" + lineBreak + "\nPopular Words\n" + popularWords.showString(popularWordsRange,100,false) + "\nCommon Words\n" + commonWords.showString(commonWordsRange,100,false) + "\nRare Words\n" + rareWords.showString(rareWordsRange,100,false) + "\n" + lineBreak + "('total number of distinct letters=', " + letterLength + ")\n('popular_threshold_letter=', " + popularLetterThreshold + ")\n('common_threshold_l_letter=', " + commonLowerLetterThreshold + ")\n('common_threshold_u_letter=', " + commonUpperLetterThreshold + ")\n('rare_threshold_letter=', " + rareLetterThreshold + ")" + lineBreak + "\nPopular Letters\n" + popularLetters.showString(popularLettersRange,100,false) + "\nCommon Letters\n" + commonLetters.showString(commonLettersRange,100,false) + "\nRare Letters\n" + rareLetters.showString(rareLettersRange,100,false) + lineBreakEnd;
		
		
		//write the file to output-[sample name]
		
		if(amazonEMR == false) {
			//write file to local directory if running on local cluster
			try(PrintWriter output = new PrintWriter("output-" + fileName)){
				output.println(outputString);
			} catch(FileNotFoundException e) {
			
			}
		} else {
			//write file to S3 Bucket if running on AWS EMR
			client.putObject(bucketName, "output-"+bucketItemKey,outputString);
		}
		
		//end application
		spark.close();
		System.out.println("--------------------------");
		System.out.println("APPLICATION END");		
		
	}
	
}
