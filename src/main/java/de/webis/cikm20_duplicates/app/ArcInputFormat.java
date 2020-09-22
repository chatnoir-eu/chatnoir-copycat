package de.webis.cikm20_duplicates.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;

import com.amazonaws.services.s3.model.AmazonS3Exception;

import lombok.SneakyThrows;

public class ArcInputFormat extends FileInputFormat<LongWritable, ARCRecord> {
	@Override
	public RecordReader<LongWritable, ARCRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ArcRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	public static class ArcRecordReader extends RecordReader<LongWritable, ARCRecord> {

		private LongWritable key = null;
		private ARCRecord value = null;
		private InputStream in;
		private Iterator<ArchiveRecord> iter;

		@Override
		public void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public ARCRecord getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0.0f;
		}

		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			final Path file = split.getPath();

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());

			fileIn.seek(split.getStart());
			in = fileIn;

			initialize(file.toString(), fileIn);
		}

		public void initialize(String path, InputStream is) {
			key = null;
			value = null;
			iter = MyARCReaderFactory.getIteratorOrEmptyIterator(new Path(path), is);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			try {
				if (!iter.hasNext()) {
					key = null;
					value = null;
	
					return false;
				} else {
					value = (ARCRecord) iter.next();
	
					if (value != null) {
						if (key == null) {
							key = new LongWritable();
						}
						key.set(value.getPosition());
					}
	
					return true;
	
				}
			} catch (Exception e) {
//				FIXME: REMOVE THIS
				return false;
			}
		}
	}

	public static class MyARCReaderFactory extends ARCReaderFactory {
		private static MyARCReaderFactory factory = new MyARCReaderFactory();

		@SneakyThrows
		public static Iterator<ArchiveRecord> getIteratorOrEmptyIterator(Path path, InputStream is) {
			try {
				return ((ARCReader) factory.getArchiveReader(path.toString(), is, Boolean.TRUE)).iterator();
			} catch (Exception e) {
//				FIXME: REMOVE THIS
				return Collections.emptyIterator();
			}
		}
	}
}
