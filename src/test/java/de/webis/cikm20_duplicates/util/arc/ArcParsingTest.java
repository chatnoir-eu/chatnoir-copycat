package de.webis.cikm20_duplicates.util.arc;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.approvaltests.Approvals;
import org.archive.io.arc.ARCRecord;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import de.webis.cikm20_duplicates.util.arc.ARCParsingUtil;

public class ArcParsingTest extends SharedJavaSparkContext {
	@Test
	public void approveUrlsWithin2008SampleArcFile() {
		String input = "src/test/resources/data/cc-arc-sample/small-sample-1213893279526_0.arc";
		JavaPairRDD<LongWritable, ARCRecord> records = ARCParsingUtil.records(jsc(), input);
		List<String> actual = records.map(i -> ARCParsingUtil.extractURL(i._2())).collect();

		Approvals.verifyAsJson(actual);
	}

	@Test
	public void approveUrlsWithin2009SampleArcFile() {
		String input = "src/test/resources/data/cc-arc-sample/small-sample-1253240302521_8.arc";

		JavaPairRDD<LongWritable, ARCRecord> records = ARCParsingUtil.records(jsc(), input);
		List<String> actual = records.map(i -> ARCParsingUtil.extractURL(i._2())).collect();

		Approvals.verifyAsJson(actual);
	}

	@Test
	public void approveUrlsWithin2010SampleArcFile() {
		String input = "src/test/resources/data/cc-arc-sample/small-sample-1262850551027_8.arc";

		JavaPairRDD<LongWritable, ARCRecord> records = ARCParsingUtil.records(jsc(), input);
		List<String> actual = records.map(i -> ARCParsingUtil.extractURL(i._2())).collect();

		Approvals.verifyAsJson(actual);
	}

//	FIXME: Test that fails when hadoop home is not set!
}
