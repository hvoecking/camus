package com.linkedin.camus.etl.kafka.common;


import java.io.OutputStream;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import java.io.IOException;
import java.io.DataOutputStream;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

public class MbrAvroRecordWriterProvider implements RecordWriterProvider {

  private String extension = "";
  private boolean isCompressed = false;
  private CompressionCodec codec = null;

  public MbrAvroRecordWriterProvider(TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();

    isCompressed = FileOutputFormat.getCompressOutput(context);

    if (isCompressed) {
      //Class<? extends CompressionCodec> codecClass = null;
//      if ("snappy".equals(EtlMultiOutputFormat.getEtlOutputCodec(context))) {
//        codecClass = SnappyCodec.class;
//      } else if ("gzip".equals((EtlMultiOutputFormat.getEtlOutputCodec(context)))) {
//        codecClass = GzipCodec.class;
//      } else {
//        codecClass = DefaultCodec.class;
//      }
//      codec = ReflectionUtils.newInstance(codecClass, conf);
      codec = new CompressionCodecFactory(context.getConfiguration()).getCodecByName(EtlMultiOutputFormat.getEtlOutputCodec(context));
      extension = codec.getDefaultExtension();
    }
  }

  @Override
  public String getFilenameExtension() {
    return extension;
  }

  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
      CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {

    // Get the filename for this RecordWriter.
    Path path =
        new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

    FileSystem fs = path.getFileSystem(context.getConfiguration());
    OutputStream out;
    if (!isCompressed) {
      out = fs.create(path, false);
//      return new ByteRecordWriter(fileOut, recordDelimiter);
    } else {
      out = new DataOutputStream(codec.createOutputStream(fs.create(path, false)));
//      return new ByteRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)), recordDelimiter);
    }
    final DataFileWriter<Object> writer = new DataFileWriter<Object>(new SpecificDatumWriter<Object>());
    writer.create(((GenericRecord) data.getRecord()).getSchema(), out);
    writer.setSyncInterval(EtlMultiOutputFormat.getEtlAvroWriterSyncInterval(context));
    return new RecordWriter<IEtlKey, CamusWrapper>() {
      @Override
      public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
        writer.append(data.getRecord());
								            }

      @Override
      public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
        writer.close();
      }
    };

  }
}
