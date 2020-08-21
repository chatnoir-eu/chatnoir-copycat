package de.webis.cikm20_duplicates.app;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.chatnoir2.mapfile_generator.inputformats.WarcInputFormat;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import lombok.experimental.UtilityClass;
import net.sourceforge.argparse4j.inf.Namespace;

@UtilityClass
public class WARCParsingUtil {
	public static JavaPairRDD<LongWritable, WarcRecord> records(JavaSparkContext sc, Namespace parsedArgs) {
		Class<? extends WarcInputFormat> inputFormat = ArgumentParsingUtil.InputFormats.valueOf(parsedArgs.get(ArgumentParsingUtil.ARG_FORMAT)).getInputFormat();
		
		return sc.newAPIHadoopFile(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT), inputFormat, LongWritable.class, WarcRecord.class, sc.hadoopConfiguration());
	}
}
