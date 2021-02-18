package de.webis.cikm20_duplicates.app;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.spark.SparkCreateIdsToRemove;
import de.webis.cikm20_duplicates.spark.SparkCreateIdsToRemove.KeepId;
import lombok.AllArgsConstructor;
import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class CreateIdsToRemove {
	
	private static String ARG_EXACT_DUPLICATES = "exactDuplicateInputs",
						  ARG_NEAR_DUPLICATES = "nearDuplicateInputs";
	
	public static void main(String[] args) {
		IdsToRemoveConfiguration config = parseArgs(args);

		if (config == null) {
			return;
		}
		KeepId keepIds = config.getKeepIds();
		
		try (JavaSparkContext context = context()) {
			for(int i=0; i<config.getExactDuplicateInputs().size(); i++) {
				JavaRDD<String> exactDuplicates = context.textFile(config.getExactDuplicateInputs().get(i));
				JavaRDD<String> nearDuplicates = context.textFile(config.getNearDuplicateInputs().get(i));
				
				SparkCreateIdsToRemove.idsToRemove(nearDuplicates, exactDuplicates, keepIds)
					.repartition(1)
					.saveAsTextFile(config.getOutputs().get(i), BZip2Codec.class);
			}
			
			JavaRDD<String> combinedInputs = context.parallelize(Arrays.asList());

			
			for(int i=0; i<config.getExactDuplicateInputs().size(); i++) {
				combinedInputs.union(context.textFile(config.getOutputs().get(i)));
			}
			
			combinedInputs.distinct()
				.repartition(1)
				.saveAsTextFile(config.getOutputs().get(config.getOutputs().size() -1), BZip2Codec.class);
		}
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName(ArgumentParsingUtil.TOOL_NAME + ": CreateIdsToRemove");

		return new JavaSparkContext(conf);
	}
	
	public static IdsToRemoveConfiguration parseArgs(String...args) {
		Namespace parsedArgs = validArgumentsOrNull(args);
		
		if(parsedArgs == null) {
			return null;
		}
		
		return new IdsToRemoveConfiguration(parsedArgs);
	}
	
	@Data
	@AllArgsConstructor
	static class IdsToRemoveConfiguration {
		private final List<String> exactDuplicateInputs, nearDuplicateInputs, outputs;
		private final String keepIds;
		
		public IdsToRemoveConfiguration(Namespace parsedArgs) {
			this(parsedArgs.getList(ARG_EXACT_DUPLICATES), parsedArgs.getList(ARG_NEAR_DUPLICATES), parsedArgs.getList(ArgumentParsingUtil.ARG_OUTPUT), parsedArgs.getString("keepIds"));
			
			failWhenConfigurationIsInvalid();
		}

		public KeepId getKeepIds() {
			if("CW09".equals(keepIds)) {
				return SparkCreateIdsToRemove.CLUEWEB09;
			} else if ("CW12".equals(keepIds)) {
				return SparkCreateIdsToRemove.CLUEWEB12;
			} else if ("CC".equals(keepIds)) {
				return SparkCreateIdsToRemove.COMMON_CRAWL;
			} else if ("ALL".equals(keepIds)) {
				return SparkCreateIdsToRemove.ALL_CRAWLS;
			} else if ("CW09b".equals(keepIds)) {
				return SparkCreateIdsToRemove.idsToKeepFromFile("/mnt/ceph/storage/data-in-progress/data-research/web-search/SIGIR-21/sigir21-deduplicate-trec-run-files/third-party/ids-in-clueweb09b");
			} else if ("CW12b".equals(keepIds)) {
				return SparkCreateIdsToRemove.idsToKeepFromFile("/mnt/ceph/storage/data-in-progress/data-research/web-search/SIGIR-21/sigir21-deduplicate-trec-run-files/third-party/ids-in-clueweb12b13");
			}

			throw new RuntimeException("Could not handle " + keepIds);
		}

		private void failWhenConfigurationIsInvalid() {
			if(exactDuplicateInputs == null || exactDuplicateInputs.size() < 1 
					|| nearDuplicateInputs == null || nearDuplicateInputs.size() != exactDuplicateInputs.size() 
					|| outputs == null || (outputs.size() -1) != nearDuplicateInputs.size()) {
				throw new RuntimeException("Illegal Configuration");
			}
			
			Set<String> paths = new HashSet<>(exactDuplicateInputs);
			paths.addAll(nearDuplicateInputs);
			paths.addAll(outputs);
			
			if(paths.size() != (3*nearDuplicateInputs.size())+1) {
				throw new RuntimeException("You have specified redundant output paths.");
			}
		}
	}
	
	static Namespace validArgumentsOrNull(String[] args) {
		ArgumentParser parser = argParser();

		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}

	private static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers
			.newFor(ArgumentParsingUtil.TOOL_NAME + ": CreateIdsToRemove")
			.addHelp(Boolean.TRUE).build();

		ret.addArgument("--" + ARG_EXACT_DUPLICATES)
			.required(Boolean.TRUE)
			.nargs("*")
			.help("The input paths that contain exact-duplicate files (parallel to " + ARG_NEAR_DUPLICATES +").");

		ret.addArgument("--" + ARG_NEAR_DUPLICATES)
			.required(Boolean.TRUE)
			.nargs("*")
			.help("The input paths that contain the near-duplicate files (parrallel to " + ARG_EXACT_DUPLICATES + ").");

		ret.addArgument("-o", "--" + ArgumentParsingUtil.ARG_OUTPUT)
			.required(Boolean.TRUE)
			.nargs("*")
			.help("The output paths (you must specify one more output path as " + ARG_EXACT_DUPLICATES + ").");

		ret.addArgument("--keepIds")
			.required(Boolean.TRUE)
			.choices(Arrays.asList("ALL", "CC", "CW12", "CW09", "CW12b", "CW09b"))
			.type(String.class);
		
		return ret;
	}
}
