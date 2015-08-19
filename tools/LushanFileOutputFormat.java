import java.io.IOException;
import java.io.DataOutputStream;
import java.io.BufferedOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

public class LushanFileOutputFormat 
extends FileOutputFormat<WritableComparable, Writable> {

	protected static class LushanRecordWriter<WritableComparable, Writable> 
		implements RecordWriter<WritableComparable, Writable> {
			
		private long size = 0;
		private long offset = 0;
		private LongWritable lastKey = new LongWritable();

		private boolean compress = false;
		private CompressionCodec codec = null;
		private CompressionOutputStream deflateFilter = null;
		private DataOutputStream deflateOut = null;
		private Compressor compressor = null;

		protected DataOutputBuffer buffer = new DataOutputBuffer();
		protected Serializer uncompressedValSerializer = null;
		protected Serializer compressedValSerializer = null;
		
		public static final String INDEX_FILE_NAME = "idx";
		public static final String DATA_FILE_NAME = "dat";

		protected FSDataOutputStream dataOut;
		protected FSDataOutputStream indexOut;

		public LushanRecordWriter(Configuration conf, FileSystem fs, String dirName, 
				Class valClass, CompressionCodec codec, Progressable progress) throws IOException {

			this.codec = codec;

			SerializationFactory serializationFactory = new SerializationFactory(conf);
			if (this.codec != null) {
				this.compress = true;
				this.compressor = CodecPool.getCompressor(this.codec);
				this.deflateFilter = this.codec.createOutputStream(buffer, compressor);

				this.deflateOut = 
					new DataOutputStream(new BufferedOutputStream(deflateFilter));
				this.compressedValSerializer = serializationFactory.getSerializer(valClass);
				this.compressedValSerializer.open(deflateOut);
			} else {
				this.uncompressedValSerializer = serializationFactory.getSerializer(valClass);
				this.uncompressedValSerializer.open(buffer);
			}

			Path dir = new Path(dirName);
			if (!fs.mkdirs(dir)) {
				throw new IOException("Mkdirs failed to create directory " + dir.toString());
			}
			Path dataFile = new Path(dir, DATA_FILE_NAME);
			Path indexFile = new Path(dir, INDEX_FILE_NAME);

			this.dataOut = fs.create(dataFile, progress);
			this.indexOut = fs.create(indexFile, progress);
		}

		private void checkKey(LongWritable key) throws IOException {

			// check that keys are well-ordered
			if (size != 0 && lastKey.get() > key.get())
				throw new IOException("key out of order: "+key+" after "+lastKey);

			lastKey.set(key.get());
		}
			
          

		public synchronized void write(WritableComparable key, Writable value) 
			throws IOException {

			if (!(key instanceof LongWritable)) {
				throw new IOException("key is not instanceof LongWritable");
			}

			buffer.reset();

			LongWritable k = (LongWritable)key;
			checkKey(k);
			offset = dataOut.getPos();
			if (offset > 0xFFFFFFFFFFL) return;

			if (compress) {
				deflateFilter.resetState();
				compressedValSerializer.serialize(value);
				deflateOut.flush();
				deflateFilter.finish();
			} else {
				uncompressedValSerializer.serialize(value);
			}

			long length = buffer.getLength();
			if (length > 0x2FFFFFL) return;

			long pos = (length << 40) | offset;
			dataOut.write(buffer.getData(), 0, buffer.getLength());

			indexOut.writeLong(Long.reverseBytes(k.get()));
			indexOut.writeLong(Long.reverseBytes(pos));

			size++;
		}
		
		public synchronized void close(Reporter reporter) throws IOException {

			if (uncompressedValSerializer != null) {
				uncompressedValSerializer.close();
			}
			
			if (compressedValSerializer != null) {
				compressedValSerializer.close();
				CodecPool.returnCompressor(compressor);
				compressor = null;
			}

			dataOut.close();
			indexOut.close();
		}
	}

	public RecordWriter<WritableComparable, Writable> getRecordWriter(FileSystem ignored, JobConf job,
                                      String name, Progressable progress)
		throws IOException {
			// get the path of the temporary output file 
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			
			FileSystem fs = file.getFileSystem(job);
			CompressionCodec codec = null;
			if (getCompressOutput(job)) {
				// find the right codec
				Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, 
						DefaultCodec.class);
				codec = ReflectionUtils.newInstance(codecClass, job);
			}
    
			// ignore the progress parameter, since LushanFile is local
			return new LushanRecordWriter<WritableComparable, Writable>(job, fs, file.toString(),
                         job.getOutputValueClass().asSubclass(Writable.class),
                         codec,
                         progress);

		}
}

