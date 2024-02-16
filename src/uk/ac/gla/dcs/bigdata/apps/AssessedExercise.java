package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import org.apache.spark.api.java.function.FilterFunction;


/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	private static final int DocumentRanking = 0;



	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		System.out.println("Loading queries from file: " + queriesjson);
		
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		//Dataset<NewsArticle> filteredArticles = news.filter(new NullFilter());
        //-------------------------------------------my trail
		
		
		//query print
		System.out.println("Printing content of each query:");
		queries.printSchema();
		// Collect the data to the driver node
		List<Query> queryList = queries.collectAsList();
        // Print the content of each query
		System.out.println("Printing content of each query:");
		for (Query query : queryList) {
		    System.out.println(query.getOriginalQuery());
		    System.out.println(query.getQueryTerms());
		    System.out.println(Arrays.toString(query.getQueryTermCounts()));
		 }
		//
				
		//news article 
		//news.show();		
		
		// Spark Transformation 1 filter () 
		Dataset<NewsArticle> filteredArticles = news.filter((FilterFunction<NewsArticle>) article ->
	    article.getId() != null &&
	    article.getTitle() != null &&
	    article.getContents() != null &&
	    article.getContents().stream().anyMatch(contentItem -> "paragraph".equals(contentItem.getSubtype()) || "Paragraph".equals(contentItem.getSubtype())));
		
			
		
		// Collect the data to the driver node
        for (NewsArticle article : filteredArticles.collectAsList()) {
            // Print the title
            System.out.println("Title: " + article.getTitle());

            // Print each paragraph
            for (ContentItem contentItem : article.getContents()) {
            	String paragraphContent = cleanHtml(contentItem.getContent());
             //   System.out.println("Paragraph: " + contentItem.getContent());
                System.out.println("Paragraph: " +  paragraphContent);
            }
        }
        
        //
		
		//------------------------------------------my trail ends
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// we have to put our logic here 
		//do it
		//
		//----------------------------------------------------------------
		
		return null; // replace this with the the list of DocumentRanking output by your topology
	
	}
	
	private static String cleanHtml(String html) {
	    // Check if the input HTML is null
	    if (html == null) {
	        return ""; // Return an empty string if the input is null
	    }

	    // Remove HTML tags and entities while preserving line breaks
	    return html.replaceAll("<[^>]*>", "").replaceAll("&nbsp;", " ").replaceAll("&[a-zA-Z]+;", " ").trim();
	}
}
