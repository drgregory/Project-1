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


public class SmallWorld {
    // Maximum dept for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Skeleton code uses this to share denom cmd-line arg across cluster
    public static final String DENOM_PATH = "denom.txt";

    // Example enumerated type, used by EValue and Counter example
    public static enum ValueUse {EDGE};
    
    public static enum BFSTracker{GLOBALCOUNT};
    
    public static enum Counter{C0(0L), C1(1L), C2(2L), C3(3L), C4(4L), C5(5L),
	    C6(6L), C7(7L), C8(8L), C9(9L), C10(10L), C11(11L), C12(12L), C13(13L),
	    C14(14L), C15(15L), C16(16L), C17(17L), C18(18L), C19(19L), C20(20L);
    	
    //public long count;
	public long whichCounter;
    	
    Counter(long wC) {
	//count = l;
	whichCounter = wC;
    }
    };
    public static long dCounter = 0;
    
    public static class Vertex {
	public LongWritable name;
	public ArrayList<LongWritable> distances;
	public ArrayList<LongWritable> hasSeen;
	public int numOfSearches;
	public boolean isSpecial = false;
	public static Pattern nameParse = Pattern.compile("[\\S]+");
	//public static Pattern trueParse = Pattern.compile("true");
	public static Pattern distanceParse = Pattern.compile("[$]start [[\\d] ]* [$]end");
	public static Pattern digitParse = Pattern.compile("[\\d]+");
	public static Pattern numSearchParse = Pattern.compile("[$]numSearch [\\d]+ [$]stopNumSearch");

	public Vertex(LongWritable n, int numSearches) {
	    name = n;
	    numOfSearches = numSearches;
	}
	public Vertex(Text information) {
		String s = information.toString();
		
		Matcher matchName = nameParse.matcher(s);
		matchName.find();
		this.name = new LongWritable(Long.parseLong(matchName.group(0)));
		
		/*Matcher matchBool = trueParse.matcher(s);
		  this.goToNext = matchBool.find();*/
		
		Matcher dists = distanceParse.matcher(s);
		dists.find();
		String d = dists.group(0);
		
		ArrayList<LongWritable> dis = new ArrayList<LongWritable>();
		Matcher getDists = digitParse.matcher(d);
		//int i = 0;
		while (getDists.find()) {
		    //i += 1;
		    dis.add(new LongWritable (Long.parseLong(getDists.group(0))));
		}
		this.distances = dis;

		Matcher numSearcher = numSearchParse.matcher(s);
		numSearcher.find();
		String searchNum = numSearcher.group(0);
		Matcher getThisNum = digitParse.matcher(searchNum);
		getThisNum.find();
		int howManySearches = Integer.parseInt(getThisNum.group(0));
		this.numOfSearches = howManySearches;
		/*if  (i > 0) {
		    Object[] myDistances = dis.toArray();
		    LongWritable[] finalDistances = new LongWritable[dis.size()];
		    for (int i = 0; i < dis.size(); i += 1) {
			finalDistances[i] = (LongWritable) myDistances[i];
		    }
		    this.distances = finalDistances;
		    }*/
	}
	public void setNumOfSearches(int with) {
		this.numOfSeraches = with;
	}
	public ArrayList<LongWritable> getDistances() {
		return this.distances;
	}
	public Text makeIntoText() {
		long thisName = name.get();
		String information = "";
		information += thisName + " ";
		//information += goToNext;
		information += " $numSearch " + numOfSearches + " $stopNumSearch";
		information += " $start";
		if (distances != null) {
		    for (int i = 0; i < distances.size(); i += 1) {
			information += " " + distances.get(i);
			}
		}
		information += " $end";
		Text textInfo = new Text();
		textInfo.set(information);
		return textInfo;
	}

	/**public void write(DataOutput out) throws IOException {
            out.write(
	    dist.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    name.set(in.readUTF());
	    dist.set(in.readLong());
        }

        public void setDist(long toWhat) {
	    dist.set(dist.get() + toWhat);
	}

	public void setHasTraversed(boolean toWhat) {
	    hasTraversed = toWhat;
	}

        public String toString() {
            return name.toString() + ": " + dist.get();
        }*/
    }	
	
    // Example writable type
    public static class EValue implements Writable {
        public ValueUse use;
        public long value;

        public EValue(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public EValue() {
            this(ValueUse.EDGE, 0);
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeUTF(use.name());
            out.writeLong(value);
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            use = ValueUse.valueOf(in.readUTF());
            value = in.readLong();
        }

        public void set(ValueUse use, long value) {
            this.use = use;
            this.value = value;
        }

        public String toString() {
            return use.name() + ": " + value;
        }
    }


    /* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
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

        /* Will need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            // Send edge forward only if part of random subset
            if (Math.random() < 1.0/denom) {
                context.write(key, value);
            }
            // Example of using a counter (counter tagged by EDGE)
            context.getCounter(ValueUse.EDGE).increment(1);
        }
    }

    /* Insert your mapreduces here
       (still feel free to edit elsewhere) */
    /* The second mapper . . . */
    /*public static class ProcesserMap extends Mapper<NodeValue, ArrayWritable, NodeValue, LongWritable> {

        public void map(LongWritable key, ArrayWritable value, Context context)
                throws IOException, InterruptedException {
	    if (key.dist() >= 0  && !key.hasTraversed) {
		key.setHasTraversed(true);
		for (LongWritable v : value) {
		    context.write(key, v);
		}
	    }
        }
	}*/
/* This example mapper loads in all edges but only propagates a subset.
       You will need to modify this to propagate all edges, but it is 
       included to demonstate how to read & use the denom argument.         */
    public static class LoaderMap2 extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
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

        /* Will need to modify to not lose any edges. */
        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {
            //int toBe = Math.random() < 1.0/denom ? 1 : 0;
            //int initialDist = toBe == 1 ? 0 : -1;
	    //Text keyT = new Text(key.toString() + " " + initialDist + " " + toBe + " 0");
	    //Text valueT = new Text(value.toString() + " -1 " + "0" + " 0");	    
	    //context.write(keyT, valueT);
	    context.write(key, value);
	    //if (toBe == 1) {
            	context.getCounter(ValueUse.EDGE).increment(1);
		//}
        }
    }
    public static class LoaderReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
	public long denom;
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

	@Override
        public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context) throws IOException, InterruptedException {
	    int keyNumOfSearches = Math.random() < 1.0/denom ? 1 : 0;
	    Vertex kVert = new Vertex(key, keyNumOfSearches);
	    //Text concatText = new Text();
	    //String initialString = "";
	    for (LongWritable value : values) {
		Vertex valVert = new Vertex(value, 0);
		context.write(kVert.makeIntoText(), valVert.makeIntoText());
	    	//initialString += value.toString() + " $end ";
	    }
	    //concatText.set(initialString);
	    //Object[] s = mySuccessors.toArray();
	    //int size = mySuccessors.size();
	    //LongWritable[] successors = new LongWritable[size];
	    //for (int i = 0; i < size; i += 1) {
	    //	successors[i] = (LongWritable) s[i];
	    //}
	    //ArrayWritable writableSuccessors = new ArrayWritable(org.apache.hadoop.io.LongWritable, successors);
	    //Text theName = new Text();
	    //theName.set(key.toString());
	    //NodeValue newKey = new NodeValue(theName, -1, mySuccessors);
	    //context.write(key, concatText);
	}
    }
    public static class BFSMapper extends Mapper<Text, Text, Text, Text> {
    	
    	public Pattern p = Pattern.compile("[\\S]+");
    	public Pattern textDelimiter = Pattern.compile("[\\S]+ [\\S]+ [\\S]+ [\\S]+ [$]end ");
    	public int getDistance(Text source) {
    		String s = source.toString();
    		Matcher m = p.matcher(s);
    		m.find();
		m.find();
    		return Integer.parseInt(m.group(0));
    	}
    	public int getToBeTraversed(Text source) {
    		String s = source.toString();
    		Matcher m = p.matcher(s);
    		m.find();
		m.find();
		m.find();
    		return Integer.parseInt(m.group(0));
    	}
    	public int getHasBeenTraversed(Text source) {
	    String s = source.toString();
    		Matcher m = p.matcher(s);
    		m.find();
		m.find();
		m.find();
		m.find();
    		return Integer.parseInt(m.group(0));
    	}
	public int getHasBeenTraversed(String s) {
    		Matcher m = p.matcher(s);
    		m.find();
		m.find();
		m.find();
		m.find();
    		return Integer.parseInt(m.group(0));
    	}
    	public int getName(String s) {
    		Matcher m = p.matcher(s);
    		m.find();
    		return Integer.parseInt(m.group(0));
    	}

	public Pattern special = Pattern.compile("[$]search");
	public String isSpecial = "$search";
	
	
	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException {
	Vertex keyV = new Vertex(key);
	Vertex valueV = new Vertex(value);
	int searchNum = keyV.getNumOfSearches();
	if (searchNum) {
		key = key.setNumOfSearches(0);
		Text howManySearches = new Text(searchSign + searchNum);
		Text k = keyV.makeIntoText();
		Text v = valueV.makeIntoText();
		context.write(v, howManySearches);
		context.write(k, v);
	} else {
		context.write(keyV.makeIntoText(), valueV.makeIntoText());
	}
}
    	/*public void map(Text key, Text value, Context context)
    		throws IOException, InterruptedException {
	    //Vertex v1 = new Vertex(key);
	    //Vertex v2 = new Vertex(value);
	    Matcher m = textDelimiter.matcher(value.toString());
    		Boolean search = getToBeTraversed(key) == 1 /*&& getHasBeenTraversed(key) == 0;
    		if (search) {
    			String whatToChange = key.toString();
    			whatToChange = whatToChange.trim();
    			whatToChange = whatToChange.substring(0, whatToChange.length() - 1).concat("1");
    			key.set(whatToChange);
    			while (m.find()) {
    				String s = m.group(0);
    				s = s.substring(0, s.length() - 6);
    				Text t = new Text(s);
    				Text specialSearch = new Text(isSpecial + getDistance(key));
    				context.write(t, specialSearch);
    				context.write(key, t);
    			}
    		} else {
    			while (m.find()) {
    				String current = m.group(0);
    				current = current.substring(0, current.length() - 6);
    				Text outputValue = new Text(current);
    				context.write(key, outputValue);
    			}
    		}
    		}*/
    }
    public static class BFSReducer extends Reducer<Text, Text, Text, Text> {

    	public Pattern finder = Pattern.compile("[\\S]+");
	
	public String getName(Text source) {
	    String s = source.toString();
	    Matcher m = finder.matcher(s);
	    m.find();
	    return m.group(0);
    	}
    	
    	public int getHasBeenTraversed(Text source) {
	    String s = source.toString();
    		Matcher m = finder.matcher(s);
    		m.find();
		m.find();
		m.find();
		m.find();
    		return Integer.parseInt(m.group(0));
    	}
    	
    	public int getDistance(Text source) {
	    String s = source.toString();
    		Matcher m = finder.matcher(s);
    		m.find();
    		m.find();
    		return Integer.parseInt(m.group(0));
    	}

	Pattern p = Pattern.compile("[$]search[\\d]+");
	
	public static Pattern special = Pattern.compile("[$][$][$][\\d]+");
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	int numOfSearches = 0;
	Vertex keyV = new Vertex(key);
	for (Text t : values) {
		if(t.find("[$][$][$][\\d]+")) {//Special character for searchnum
	String s = t.toString();
	Matcher extract = special.matcher(s);
	extract.find();
	String isolatedPart = extract.group(0);
	Matcher getDig = Pattern.compile("[\\d]+").matcher(isolatedPart);
	getDig.find();
	numOfSearches += Integer.parseInt(getDig.group(0));
		}	
	}
	keyV.setNumOfSearches(numOfSearches);
	Text keyT = keyV.makeIntoText();
	for (Text t : values) {
		if(!t.find("[$][$][$][\\d]+")) { //Same special character
			context.write(keyT, t);
		}	
	}
}	
	/*@Override
        public void reduce(Text key, Iterable<Text> values,
			   Context context) throws IOException, InterruptedException {
	    Boolean searchFrom = false;
	    String concatVals = "";
	    int distance = getDistance(key);
	    for (Text v : values) {
	    	String c = v.toString();
	    	Matcher m = p.matcher(c);
	    	if (m.matches()) {
	    		searchFrom = true;
	    		c = c.substring(7);
	    		//dataFinishedCounter += 1;
	    		distance = Integer.parseInt(c) + 1;
	    	} else {
	    		concatVals += c + " $end ";
	    	}
	    }
	    long num = dCounter;/*context.getCounter(BFSTracker.GLOBALCOUNT).getValue();*/
	    //System.out.println("!" + num + "!");
	    /*Counter thisCounter = null;
	    for (Counter c : Counter.values()) {
           	if (c.whichCounter == num) {
           		thisCounter = c;
           		break;
           	}
	    }
	    System.out.println(thisCounter);
	    if (thisCounter != null) {
		//Reporter.incrCounter(thisCounter, 1);
		//context.getCounter(thisCounter).increment(1);
	    /*}
	    String k = getName(key);
	    k += " " + distance;
	    k += searchFrom ? " 1 " : " 0 ";
	    k += getHasBeenTraversed(key);
	    Text finalKey = new Text(k);
	    Text finalVals = new Text(concatVals);
	    context.write(finalKey, finalVals);
	}*/
    }
    public static class CleanupMapper extends Mapper<Text, Text, LongWritable, LongWritable> {
    	
    	public static final LongWritable ONE = new LongWritable(1L);
    	public Pattern textDelimiter = Pattern.compile("[\\S]+ [\\S]+ [\\S]+ [\\S]+ [$]end ");
    	public Pattern p = Pattern.compile("[\\S]+");

	public long getDistance(Text source) {
    		String s = source.toString();
    		Matcher m = p.matcher(s);
    		m.find();
		m.find();
    		return Long.parseLong(m.group(0));
    	}
    	
    	public void map(Text key, Text values, Context context)
    		throws IOException, InterruptedException {
    		Matcher m = textDelimiter.matcher(values.toString());
    		/*while (m.find()) {
    			long thisDist = getDistance(key);
    			LongWritable distKey = new LongWritable(thisDist);
    			context.write(distKey, ONE);
			}*/
		for (Counter c : Counter.values()) {
		    long l = context.getCounter(c).getValue();
		    if (l != 0) {
			LongWritable lw = new LongWritable(l);
	    
			context.write(new LongWritable(c.whichCounter), lw);
		    }
    		}
	}
    }
    public static class CleanupReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
	@Override
        public void reduce(LongWritable key, Iterable<LongWritable> values,
			   Context context) throws IOException, InterruptedException {
	    //if (key.get() >= 0) {
	    //		long sum = 0L;
	    	for (LongWritable value : values) {
		    context.write(key, value);
		    //sum += value.get();
	    	}
		//LongWritable finalSum = new LongWritable(sum);
	    	//context.write(key, finalSum);
		//	}
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
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(LoaderMap2.class);
        job.setReducerClass(LoaderReducer.class);

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
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(BFSMapper.class);
            job.setReducerClass(BFSReducer.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

	    //i = dataFinishedCounter > 0 ? i : MAX_ITERATIONS;

            job.waitForCompletion(true);
            i++;
	    job.getCounters().findCounter(BFSTracker.GLOBALCOUNT).increment(1);
	    dCounter += 1;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(CleanupMapper.class);
        job.setCombinerClass(CleanupReducer.class);
        job.setReducerClass(CleanupReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
