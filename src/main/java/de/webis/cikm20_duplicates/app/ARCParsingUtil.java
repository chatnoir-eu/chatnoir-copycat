package de.webis.cikm20_duplicates.app;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.Header;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCRecord;

import de.webis.cikm20_duplicates.app.ArcInputFormat.MyARCReaderFactory;
import net.sourceforge.argparse4j.inf.Namespace;

public class ARCParsingUtil {

	public static JavaPairRDD<LongWritable, ARCRecord> records(JavaSparkContext jsc, Namespace parsedArgs) {
		return records(jsc, parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
	}
	
	public static JavaPairRDD<LongWritable, ARCRecord> records(JavaSparkContext jsc, String input) {
		return jsc.newAPIHadoopFile(input, ArcInputFormat.class, LongWritable.class, ARCRecord.class, jsc.hadoopConfiguration());
	}

	public static String extractURL(ARCRecord record) {
		if(record == null) {
			return "NOT-AVAILABLE";
		}
		
		 Header[] headers = record.getHttpHeaders();
		 if(headers == null) {
			 return "NOT-AVAILABLE"; 
		 }
		 
		 for(Header header: headers) {
			 if("x_commoncrawl_OriginalURL".equalsIgnoreCase(header.getName())) {
				 return header.getValue();
			 }
		 }
		 
		 return "ORIGINAL_URL_NOT_AVAILABLE";
	}

	public static List<String> extractAllURLs(String file, InputStream is) {
		ARCReader reader = MyARCReaderFactory.get(new org.apache.hadoop.fs.Path(file), is);
		List<String> ret = new ArrayList<>();
		
		reader.forEach(i -> ret.add(extractURL((ARCRecord) i)));
		
		return ret;
	}
}
