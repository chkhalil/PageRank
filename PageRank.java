
package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

//import src.shavadoop.WriteFile;

/*
 * VERY IMPORTANT 
 * 
 * Each time you need to read/write a file, retrieve the directory path with conf.get 
 * The paths will change during the release tests, so be very carefully, never write the actual path "data/..." 
 * CORRECT:
 * String initialVector = conf.get("initialRankVectorPath");
 * BufferedWriter output = new BufferedWriter(new FileWriter(initialVector + "/vector.txt"));
 * 
 * WRONG
 * BufferedWriter output = new BufferedWriter(new FileWriter(data/initialVector/vector.txt"));
 */

public class PageRank {
	
	public static void createInitialRankVector(String directoryPath, long n) throws IOException 
	{
		
		File dir = new File(directoryPath);
	    if (dir.exists()){						// delete directory if exists
	    	FileUtils.deleteQuietly(dir);
	    	}
	    System.out.println("try to create initial file dir");
		  // create the directory
		
		try{
			dir.mkdir();  
			System.out.println(" directory created");
	    } 
	    catch(SecurityException se){
	       System.out.println(" failed to create dir");
	    }        
		
		File file = new File(directoryPath+"/file");  // create the initial vector file
		if (!file.exists()) {				// create the new file
			file.createNewFile();
		}
		
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		for (int i=1;i<=n;i++){
			bw.write(i+ " "+ new Double(1.0 / n).toString());
			bw.newLine();
		}
		
		bw.close();
		
	}
	
	
	public static ArrayList<String> Read_rank_vector(String filename) throws IOException{
		 
		 BufferedReader br = new BufferedReader(new FileReader(filename));
		 try {
			 
			 StringBuilder sb = new StringBuilder();
			 String line = br.readLine();
			 ArrayList<String> ranks = new ArrayList<String>();
			 
			 String rank = new String();
			 while(line != null){
				 
				 //Splitted_lines.add(line);
				 String word[] = line.split(" ");
				 rank = word[0];
				 ranks.add(rank);
				 line = br.readLine();
			 }
			return ranks;
		 } finally {
			 br.close();
			 
		 }
	 }
	
	
	public static double L_1(ArrayList<String> init_rank_vector,ArrayList<String> current_rank_vector)
	{
		double res = 0;
		// this computation suppose that init_rank_vector and current_rank_vector have the same size
		for(int i=0;i<init_rank_vector.size();i++)
		{
			res += Double.parseDouble(init_rank_vector.get(i)) - Double.parseDouble(current_rank_vector.get(i));
		}
		return res;
	}
	
	public static boolean checkConvergence(String initialDirPath, String iterationDirPath, double epsilon) throws IOException
	{
		
		
		// read n-1 vector
		ArrayList<String> init_rank_vector = new ArrayList<String>();
		init_rank_vector = PageRank.Read_rank_vector(initialDirPath);
		
		// read n vector
		ArrayList<String> current_rank_vector = new ArrayList<String>();
		current_rank_vector = PageRank.Read_rank_vector(initialDirPath);
		
		double res = PageRank.L_1(init_rank_vector,current_rank_vector);
		if(res<epsilon)
			return true;
		else
			return false;
		
		
		
	}
	
	public static void avoidSpiderTraps(String vectorDirPath, long nNodes, double beta) 
	{
		//TO DO
		
	}
	
	public static void iterativePageRank(Configuration conf) 
			throws IOException, InterruptedException, ClassNotFoundException
	{
		
		
		String initialVector = conf.get("initialVectorPath");
		String currentVector = conf.get("currentVectorPath");
		
		String finalVector = conf.get("finalVectorPath"); 
		/*here the testing system will search for the final rank vector*/
		
		Double epsilon = conf.getDouble("epsilon", 0.1);
		Double beta = conf.getDouble("beta", 0.8);

 
		//TO DO
		
		//Launch remove dead ends jobs
		RemoveDeadends.job(conf);   
		
		//Create initial vector
		Long nNodes = conf.getLong("numNodes", 1);
		createInitialRankVector(initialVector, conf.getLong("numNodes", 1));
		
		
		boolean test = false;
		
		while (!test)
		{
		// multiply the first vector by the stochastic matrix
		MatrixVectorMult.job(conf);
		
		test = checkConvergence(initialVector,currentVector,epsilon);
		// to retrieve the number of nodes use long nNodes = conf.getLong("numNodes", 0); 

		if (!test)
		{
			 FileUtils.deleteQuietly(new File(initialVector));  // remove init vector to ovewrite it
			 FileUtils.moveFile(new File(currentVector+"/part-r-00000"), new File(initialVector+"/part-r-00000"));   // current vector become init_vector
			 FileUtils.deleteQuietly(new File(currentVector));  // remove current vector to overwrite it
		}
		}

		// when you finished implementing delete this line
		//throw new UnsupportedOperationException("Implementation missing");
		
	}
}

