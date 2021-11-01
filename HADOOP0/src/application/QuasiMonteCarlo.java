package application;

import map.MapReduce;
import ordo.JobImpl;

import java.util.Random;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import formats.KVFormat;
import hdfs.HdfsClient;

public class QuasiMonteCarlo implements MapReduce {
    private static final long serialVersionUID = 1L;

    @Override
    public void map(FormatReader reader, FormatWriter writer) {
    	int squarePoints = 0;
    	int circlePoints = 0;
    	
    	/* Reading the generated x and y from the file */
    	KV kv;
        while ((kv = reader.read()) != null) {
            String tokens[] = kv.v.split(",");
            double x = Double.valueOf(tokens[0]);
            double y = Double.valueOf(tokens[1]);
            
            /* Distance between (x, y) from the origin */
            double originDist= x*x + y*y;
          
            /* Checking if (x, y) lies inside the circle */
            if (originDist <= 1){
                circlePoints+= 1;
            }
            squarePoints+= 1;
            
            writer.write(new KV("square",Double.toString(squarePoints)));
            writer.write(new KV("circle",Double.toString(circlePoints)));
        }
    }
    
    @Override
    public void reduce(FormatReader reader, FormatWriter writer) {
    	KV kv;
    	int squarePoints = 0;
    	int circlePoints = 0;
    	while ((kv = reader.read()) != null) {
    		if (kv.k.equals("square")) {
    			squarePoints += Double.valueOf(kv.v);
    		} else {
    			circlePoints += Double.valueOf(kv.v);
    		}
    	}
    	writer.write(new KV("square", Double.toString(squarePoints)));
    	writer.write(new KV("circle", Double.toString(circlePoints)));	  
    }
    
    public static void reduceFinal(FormatReader reader, FormatWriter writer) {
    	KV kv;
    	int squarePoints = 0;
    	int circlePoints = 0;
    	while ((kv = reader.read()) != null) {
    		if (kv.k.equals("square")) {
    			squarePoints += Double.valueOf(kv.v);
    		} else {
    			circlePoints += Double.valueOf(kv.v);
    		}
    	}
    	writer.write(new KV("square", Double.toString(squarePoints)));
    	writer.write(new KV("circle", Double.toString(circlePoints)));    	
    }
    
    public static void main (String args[]) {
    	
    	int nbPoints = Integer.parseInt(args[0]);
    	
    	String dir = "/home/mwissad/2A/Hidoop/hidoop/data/";
    	String dir2 = "/home/mwissad/nosave/";
    	String fname = "PointsMonteCarlo.txt";
    	
    	Format writer = new KVFormat(dir+fname);
    	writer.open(Format.OpenMode.W);
    	
    	Random r = new Random();
    	
    	double randX, randY;
    	
    	/* On mettra des lettres au début des points pour faciliter les maps par ordre alphabétique */
    	String alphabet = "abcdefghijklmnopqrstuvwxyz";
    	int len = alphabet.length();
    	   	
    	for (int i = 0; i < nbPoints; i++) {
    		/* Generate a random double between -1 and 1 */
    		randX = 2 * r.nextDouble() - 1;
    		randY = 2 * r.nextDouble() - 1;
    		writer.write(new KV((" " + alphabet.charAt(i%len))+"Point"+i, randX + "," + randY));    		
    	}
    	writer.close();
    	
    	String[] cmd = {"write","kv","/home/mwissad/2A/Hidoop/hidoop/data/PointsMonteCarlo.txt"};
    	HdfsClient.main(cmd);
    	
    	try {
    		JobImpl j = new JobImpl();
    		j.setInputFormat(Format.Type.KV);
            j.setInputFname(dir+fname);
            long t1 = System.currentTimeMillis();            
            j.startJob(new QuasiMonteCarlo());        
            
            Format reader = new KVFormat(dir2+fname+"-resfinal.txt");
            reader.open(Format.OpenMode.R);
            writer = new KVFormat(dir+"PiEstimation.txt");
            writer.open(Format.OpenMode.W);
            reduceFinal(reader,writer);
            reader.close();
            
            writer.open(Format.OpenMode.R);
            double squarePoints = Double.parseDouble(writer.read().v);
            double circlePoints = Double.parseDouble(writer.read().v);
            writer.close();

            long t2 = System.currentTimeMillis();
            
            System.out.println("************** Pi = "+(4*circlePoints/squarePoints)+" **************");
	        System.out.println("time in ms ="+(t2-t1));           
    		
    	} catch (Exception e) {
    		e.printStackTrace();          
    	}
    	 
    	
    	
    }

}
