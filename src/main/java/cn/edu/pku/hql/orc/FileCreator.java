package cn.edu.pku.hql.orc;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.util.Random;

/**
 * Created by huangql on 17-5-29.
 */
public class FileCreator {

    private static String CHAR_SET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static Random rand = new Random();

    private static String genRandomWord(int len) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < len; i++) {
            sb.append(CHAR_SET.charAt(rand.nextInt(CHAR_SET.length())));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        TypeDescription schema = TypeDescription.fromString("struct<id:bigint,word:string>");
        OrcFile.WriterOptions options = OrcFile.writerOptions(new Configuration()).setSchema(schema);
        options.compress(CompressionKind.NONE);
        options.stripeSize(FileUtils.ONE_MB);
        Writer writer = OrcFile.createWriter(new Path("./result.orc"), options);

        int batchSize = 1000;
        for (int id = 4000000; id < 5000000; ) {
            VectorizedRowBatch batch = schema.createRowBatch(batchSize);
            LongColumnVector idVector = (LongColumnVector) batch.cols[0];
            BytesColumnVector wordVector = (BytesColumnVector) batch.cols[1];
            for (int i = 0; i < batchSize; i++) {
                idVector.vector[i] = id++;
                wordVector.vector[i] = genRandomWord(100).getBytes();
                wordVector.length[i] = 100;
            }
            batch.size = batchSize;
            writer.addRowBatch(batch);
        }
        writer.close();
    }
}
