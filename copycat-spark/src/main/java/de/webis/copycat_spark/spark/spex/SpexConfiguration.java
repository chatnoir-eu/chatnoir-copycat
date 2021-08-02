package de.webis.copycat_spark.spark.spex;

import java.io.Serializable;

import de.webis.copycat_spark.app.ArgumentParsingUtil;
import lombok.Builder;
import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

@Data
@Builder
@SuppressWarnings("serial")
public class SpexConfiguration implements Serializable {
	private final String input, indexDirectory,
		intermediateScoreDirectory, documentMetadataDirectory,
		residualIndexDirectory, finalScoreDirectory, pairsToRecalculateInResidualIndexDirectory;
	
	private final int metadataPartitionCount, postlistThresholdForAllPairsCalculation;
	private final double threshold;
	
	public static SpexConfiguration parseSpexConfiguration(String[] args) {
		Namespace parsedArgs = parseArgs(args);
		if(parsedArgs == null) {
			return null;
		}
		
		String out = parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT);
		String threshold = parsedArgs.getString("threshold");
		
		return SpexConfiguration.builder()
			.input(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT))
			.indexDirectory(out + "/index")
			.intermediateScoreDirectory(out + "/intermediate-scores")
			.documentMetadataDirectory(out +"/document-metadata")
			.residualIndexDirectory(out + "/residual-index")
			.finalScoreDirectory(out +"/final-results")
			.threshold(Double.parseDouble(threshold))
			.pairsToRecalculateInResidualIndexDirectory(out + "/pairs-to-recalculate-in-residual-index-directory-threshold-" + threshold)
			.metadataPartitionCount(10)
			.postlistThresholdForAllPairsCalculation(1000)
			.build();
	}
	
	static Namespace parseArgs(String[] args) {
		ArgumentParser parser = argParser();
		
		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}

	static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor("CopyCat: Run a Full S3 Deduplication of a dataset.")
				.build();
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_INPUT)
			.help("Jsonl files that contain documents.")
			.required(true);
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_OUTPUT)
			.help("The output structure is here.")
			.required(true);
		
		ret.addArgument("--threshold")
			.help("The threshold to apply.")
			.required(true);
		
		return ret;
	}
}
