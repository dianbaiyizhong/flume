package org.apache.flume.sink.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class HiveDataStream extends AbstractHDFSWriter {

    public static final String WRITE_KEY = "WRITE_KEY";


    private static final Logger logger = LoggerFactory.getLogger(HiveDataStream.class);

    private String writeFormat;

    private Map<String, Writer> orcWriterMap = new ConcurrentHashMap<>();


//    private Map<String, TypeDescription> typeDescriptionMap = new ConcurrentHashMap<>();


    public void configure(Context context) {
        super.configure(context);
        writeFormat = context.getString("hdfs.writeFormat", "Orcfile");
        if (!writeFormat.toLowerCase(Locale.ROOT).equals("orcfile")) {
            throw new UnsupportedOperationException("Unsupported this type...");
        }
    }


    @Override
    public void open(String filePath) throws IOException {

        String[] pathArr = filePath.split("\\|");
        String writerKey = pathArr[1].split("\\/")[0].replaceAll("\\[|\\]", "");
        filePath = pathArr[0] + pathArr[1].split("\\/")[1];
        logger.info("HiveDataStream open...filePath:{},writerKey:{}", filePath, writerKey);
        Writer writer = null;
        // 根据不同的路劲形成不同的writer
        if (!orcWriterMap.containsKey(writerKey)) {
            Configuration conf = new Configuration();
            writer = OrcFile.createWriter(new Path(filePath),
                    OrcFile.writerOptions(conf)
                            .setSchema(OrcSchemaManager.getInstance().getSchema(writerKey))
                            .stripeSize(67108864)
                            .bufferSize(131072)
                            .blockSize(134217728)
                            .compress(CompressionKind.ZLIB)
                            .version(OrcFile.Version.V_0_12));

            orcWriterMap.put(writerKey, writer);
        }

    }


    @VisibleForTesting
    protected FileSystem getDfs(Configuration conf, Path dstPath) throws IOException {
        return dstPath.getFileSystem(conf);
    }


    @Override
    public void open(String filePath, CompressionCodec codec, SequenceFile.CompressionType cType) throws IOException {
        open(filePath);

    }

    @Override
    public void append(Event e) throws IOException {

        String key = e.getHeaders().get(WRITE_KEY);
        Writer writer = orcWriterMap.get(key);

        List<String> fieldList = Lists.newArrayList("name", "sex");

        VectorizedRowBatch batch = writer.getSchema().createRowBatch();

        List<BytesColumnVector> columnVectors = new ArrayList<>();
        for (int i = 0; i < fieldList.size(); i++) {
            columnVectors.add((BytesColumnVector) batch.cols[i]);
        }

        writeOne(batch, columnVectors, writer);

    }

    private void writeOne(VectorizedRowBatch batch, List<BytesColumnVector> columnVectors, Writer writer) throws IOException {

        int row = batch.size++;
        for (int i = 0; i < columnVectors.size(); i++) {
            BytesColumnVector bytesColumnVector = BytesColumnVector.class.cast(columnVectors.get(i));
            bytesColumnVector.setVal(row, "column".getBytes(), 0, "column".getBytes().length);
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    @Override
    public void sync() throws IOException {
        logger.info("OrcDataStream sync.............");
    }

    @Override
    public void close() throws IOException {
        logger.info("OrcDataStream close.............");

        orcWriterMap.forEach(new BiConsumer<String, Writer>() {
            @Override
            public void accept(String s, Writer writer) {
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }


}
