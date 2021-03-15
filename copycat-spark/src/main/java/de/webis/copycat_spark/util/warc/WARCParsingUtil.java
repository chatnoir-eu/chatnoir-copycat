package de.webis.copycat_spark.util.warc;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.chatnoir2.mapfile_generator.inputformats.ClueWeb09InputFormat;
import de.webis.chatnoir2.mapfile_generator.inputformats.ClueWeb12InputFormat;
import de.webis.chatnoir2.mapfile_generator.inputformats.CommonCrawlInputFormat;
import de.webis.chatnoir2.mapfile_generator.inputformats.WarcInputFormat;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import de.webis.copycat_spark.app.ArgumentParsingUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import net.sourceforge.argparse4j.inf.Namespace;

@UtilityClass
public class WARCParsingUtil {
	public static JavaPairRDD<LongWritable, WarcRecord> records(JavaSparkContext sc, Namespace parsedArgs) {
		Class<? extends WarcInputFormat> inputFormat = InputFormats.valueOf(parsedArgs.get(ArgumentParsingUtil.ARG_FORMAT)).getInputFormat();
		
		return records(sc, parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT), inputFormat);
	}
	
	public static JavaPairRDD<LongWritable, WarcRecord> records(JavaSparkContext sc, String path, Class<? extends WarcInputFormat> inputFormat) {
		return sc.newAPIHadoopFile(path, inputFormat, LongWritable.class, WarcRecord.class, sc.hadoopConfiguration());
	}
	
	@Getter
	@AllArgsConstructor
	public static enum InputFormats {
		CLUEWEB09(ClueWeb09InputFormat.class),
		CLUEWEB12(ClueWeb12InputFormat.class),
		COMMON_CRAWL(CommonCrawlInputFormat.class);
		
		private final Class<? extends WarcInputFormat> inputFormat;
		
		public static List<String> allInputFormats() {
			return Arrays.asList(InputFormats.values()).stream()
					.map(i -> i.name())
					.collect(Collectors.toList());
		}
	}
}
