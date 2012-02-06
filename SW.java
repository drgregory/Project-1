/*
  CS 61C Project1: Small World

  Name: Gregory Roberts
  Login: cs61c-il

  Name: Kevin Funkhouser
  Login: cs61c-as
*/


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.Math;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SW {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 5; /*Don't forget 20*/

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};
        
    public static class Node implements WritableComparable {
	public long name;
	public String distances;
	public String names;
	public boolean searchesInto;

	public Node() {
	    name = -1;
	    distances = "";
	    names = "";
	    searchesInto = false;
	}
	public Node(long n) {
	    name = n;
	    distances = "";
	    names = "";
	    searchesInto = false;
	}
		
	public int compareTo(Node other) {
	    boolean isEqual = false;
	    long otherName = other.name;
	    if (otherName == this.name) {
		isEqual = true;
	    } else if (otherName > this.name) {
		return -1;
	    } else if (otherName < this.name) {
		return 1;
	    }
	    
	    if (other.searchesInto != this.searchesInto) {
		return (this.searchesInto ? 1 : -1);
	    }
	    if (!this.distances.equals(other.distances)) {
		return 1;
	    }
	    if (!this.distances.equals(other.names)) {
		return 1;
	    }
	    return 0;
	    /*long thisValue = this.name;
	      long otherValue = ((Node)other).name;
	      return other.compareTo(this.name);*/
	}
	public int compareTo(Object other) {
	    if (other instanceof Node) {
		return this.compareTo((Node) other);
	    } else {
		return -1;
	    }
	}
	/*
	public boolean equals(Object o) {
	    return false;
	    }*/

	public void write(DataOutput out) throws IOException {
	    out.writeLong(name);
	    out.writeUTF(distances);
	    out.writeBoolean(searchesInto);
	    out.writeUTF(names);
	}

	public void readFields(DataInput in) throws IOException {
	    name = in.readLong();
	    distances = in.readUTF();
	    searchesInto = in.readBoolean();	    
	    names = in.readUTF();
	}

	public void addDistance(long d) {
	    distances += d + " ";
	    //System.out.println(distances);
	}
	public void addName(long n) {
	    names += n + " ";
	}
	public void searchChange(boolean b) {
	    searchesInto = b;
	}
    }



    public static class LoaderMap extends Mapper<LongWritable, LongWritable, Node, Node> {
        public long denom;

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
        @Override
	    public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
							   new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }

        /* Will need to modify to not loose any edges. */
        @Override
	    public void map(LongWritable key, LongWritable value, Context context)
	    throws IOException, InterruptedException {
	    Node keyNode = new Node(key.get());
	    Node valueNode = new Node(value.get());
            // Example of using a counter (counter tagged by EDGE)
	    /*we may need this later
	      context.getCounter(ValueUse.EDGES).increment(1);
	    */
	    context.write(keyNode, valueNode);
        }
    }
	
    public static class LoaderReduce extends Reducer<Node, Node, Node, Node> {
	public long denom;

        /* Setup is called automatically once per map task. This will
           read denom in from the DistributedCache, and it will be
           available to each call of map later on via the instance
           variable.                                                  */
        @Override
	    public void setup(Context context) {
            try {
                Configuration conf = context.getConfiguration();
                Path cachedDenomPath = DistributedCache.getLocalCacheFiles(conf)[0];
                BufferedReader reader = new BufferedReader(
							   new FileReader(cachedDenomPath.toString()));
                String denomStr = reader.readLine();
                reader.close();
                denom = Long.decode(denomStr);
            } catch (IOException ioe) {
                System.err.println("IOException reading denom from distributed cache");
                System.err.println(ioe.toString());
            }
        }

	public void reduce(Node key, Iterable<Node> values, Context context)
	    throws IOException, InterruptedException {	
	    if (Math.random() < 1.0/denom) {
		key.addDistance(0L);
		key.addName(key.name);
		key.searchesInto = true;
	    }
	    for (Node v : values) {
		context.write(key, v);
	    }
	}
    }

  public static class BFSMapper2 extends Mapper<Node, Node, Node, Node> {
			
	public void map(Node key, Node value, Context context)
	    throws IOException, InterruptedException {
	    if (key.searchesInto) {
		key.searchesInto = false;
		Node searchNode = new Node(-2);
		Scanner names = new Scanner(key.names);
		Scanner distances = new Scanner(key.distances);
		String m = "";
		while (names.hasNextLong()) {
		    long n = names.nextLong();
		    long d = distances.nextLong();
		    m += " " + d + " ";
		    //System.out.println("n: " + n + " d: " + d);
		    searchNode.addDistance(d);
		    searchNode.addName(n);
		}
		System.out.println("d#$!@#$!32     " + m + "    1423412351235");
		context.write(value, searchNode);
	    } else {
		System.out.println(key.distances + " !!!!238906410236401273894712839074891273894712308947");
	    }
	    context.write(key, value);
	}
    }

    public static class BFSReduce2 extends Reducer<Node, Node, Node, Node> {
			
	public void reduce(Node key, Iterable<Node> values, Context context)
	    throws IOException, InterruptedException {
	    ArrayList<Node> savedNodes = new ArrayList<Node>();
	    for (Node n : values) {
		if (n.name == -2) {
		    Scanner names = new Scanner(n.names);
		    Scanner distances = new Scanner(n.distances);
		    while (names.hasNextLong()) {
			long nam = names.nextLong();
			String namS = nam + "";
			long dis = distances.nextLong() + 1;
			if (!key.names.contains(namS)) {
			    key.addDistance(dis);
			    key.addName(nam);
			    key.searchChange(true);
			}
		    }
		} else {
		    savedNodes.add(n);
		}
	    }
	    for (Node x : savedNodes) {
		//key.searchesInto = true;
		//key.addDistance(1);
		context.write(key, x);
	    }
	}
    }	
	
   
    public static class CleanupMap extends Mapper<Node, Node, LongWritable, LongWritable> {
		
	public static LongWritable ONE = new LongWritable(1L);
		
	public void map(Node key, Node value, Context context)
	    throws IOException, InterruptedException {
	    Scanner s = new Scanner(key.distances);
	    //System.out.println(key.distances);
	    //System.out.println(key.name);
	    while(s.hasNextLong()) {
		long l = s.nextLong();
		context.write(new LongWritable(l), ONE);
	    }
	}
    }

    public static class CleanupReduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
	    throws IOException, InterruptedException {
	    long sum = 0L;
	    for (LongWritable v : values) {
		sum += 1L;
	    }
	    context.write(key, new LongWritable(sum));
	}
    }
    // Shares denom argument across the cluster via DistributedCache
    public static void shareDenom(String denomStr, Configuration conf) {
        try {
	    Path localDenomPath = new Path(DENOM_PATH + "-source");
	    Path remoteDenomPath = new Path(DENOM_PATH);
	    BufferedWriter writer = new BufferedWriter(
						       new FileWriter(localDenomPath.toString()));
	    writer.write(denomStr);
	    writer.newLine();
	    writer.close();
	    FileSystem fs = FileSystem.get(conf);
	    fs.copyFromLocalFile(true,true,localDenomPath,remoteDenomPath);
	    DistributedCache.addCacheFile(remoteDenomPath.toUri(), conf);
        } catch (IOException ioe) {
            System.err.println("IOException writing to distributed cache");
            System.err.println(ioe.toString());
        }
    }


    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Set denom from command line arguments
        shareDenom(args[2], conf);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
        job.setJarByClass(SW.class);

        job.setMapOutputKeyClass(SW.Node.class);
        job.setMapOutputValueClass(SW.Node.class);
        job.setOutputKeyClass(SW.Node.class);
        job.setOutputValueClass(SW.Node.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Example of reading a counter
        System.out.println("Read in " + 
			   job.getCounters().findCounter(ValueUse.EDGE).getValue() + 
                           " edges");

        // Repeats your BFS mapreduce
        int i=0;
        // Will need to change terminating conditions to respond to data
        while (i<MAX_ITERATIONS) {

            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SW.class);

            job.setMapOutputKeyClass(SW.Node.class);
            job.setMapOutputValueClass(SW.Node.class);
            job.setOutputKeyClass(SW.Node.class);
            job.setOutputValueClass(SW.Node.class);

            job.setMapperClass(BFSMapper2.class);
            job.setReducerClass(BFSReduce2.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

	    //i = dataFinishedCounter > 0 ? i : MAX_ITERATIONS;

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SW.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(CleanupMap.class);
        //job.setCombinerClass(CleanupReduce.class);
        job.setReducerClass(CleanupReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
