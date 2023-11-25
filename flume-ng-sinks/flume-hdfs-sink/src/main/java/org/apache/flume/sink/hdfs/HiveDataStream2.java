package org.apache.flume.sink.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
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
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class HiveDataStream2 extends AbstractHDFSWriter {

    public static final String WRITE_KEY = "WRITE_KEY";


    private static final Logger logger = LoggerFactory.getLogger(HiveDataStream2.class);

    private String writeFormat;

    private Writer writer;

    public void configure(Context context) {
        super.configure(context);
        writeFormat = context.getString("hdfs.writeFormat", "Orcfile");
        if (!writeFormat.toLowerCase(Locale.ROOT).equals("orcfile")) {
            throw new UnsupportedOperationException("Unsupported this type...");
        }
    }


    @Override
    public void open(String filePath) throws IOException {

//        String[] pathArr = filePath.split("\\|");
//        String writerKey = pathArr[1].split("\\/")[0].replaceAll("\\[|\\]", "");
//        filePath = pathArr[0] + pathArr[1].split("\\/")[1];
//        logger.info("HiveDataStream open...filePath:{},writerKey:{}", filePath, writerKey);
        // 根据不同的路劲形成不同的writer
        Configuration conf = new Configuration();
        writer = OrcFile.createWriter(new Path(filePath),
                OrcFile.writerOptions(conf)
                        .setSchema(OrcSchemaManager.getInstance().getSchema("orc_table"))
                        .overwrite(true));

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
    public void append(Event event) throws IOException {

        String key = event.getHeaders().get(WRITE_KEY);
        byte[] byteBody = event.getBody();
        String body = new String(byteBody, Charsets.UTF_8);

        List rowList = new Gson().fromJson(body, List.class);


        List<String> fieldList = OrcSchemaManager.getInstance().getSchema(key).getFieldNames();

        VectorizedRowBatch batch = writer.getSchema().createRowBatch(fieldList.size() + 20);

        List<BytesColumnVector> columnVectors = new ArrayList<>();
        for (int i = 0; i < fieldList.size(); i++) {
            columnVectors.add((BytesColumnVector) batch.cols[i]);
        }
        writeBatch(batch, columnVectors, rowList);
    }

    private void writeBatch(VectorizedRowBatch batch, List<BytesColumnVector> columnVectors, List<List<String>> rowList) throws IOException {

        for (List<String> line : rowList) {
            int row = batch.size++;
            for (int i = 0; i < line.size(); i++) {
                String column = line.get(i);
                BytesColumnVector bytesColumnVector = BytesColumnVector.class.cast(columnVectors.get(i));
                bytesColumnVector.setVal(row, column.getBytes(), 0, column.getBytes().length);
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }

        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }

    }

    @Override
    public void sync() throws IOException {
        logger.info("HiveDataStream sync.............");
    }

    @Override
    public void close() throws IOException {
        logger.info("HiveDataStream close.............");
        writer.close();
    }


}
