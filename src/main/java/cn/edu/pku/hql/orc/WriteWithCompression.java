package cn.edu.pku.hql.orc;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.Map;

/**
 * Created by quanlong.huang on 4/24/17.
 */
public class WriteWithCompression {
  public static void main(String[] args) throws IOException {
    String dir = "/Users/quanlong.huang/Downloads";
    TypeDescription schema = TypeDescription.fromString("struct<c1:int,c2:string,c3:string>");

    VectorizedRowBatch batch = schema.createRowBatch(100);
    LongColumnVector c1 = (LongColumnVector) batch.cols[0];
    BytesColumnVector c2 = (BytesColumnVector) batch.cols[1];
    BytesColumnVector c3 = (BytesColumnVector) batch.cols[2];
    for (int i = 0; i < 3; i++) {
      c1.vector[i] = i;
      c2.vector[i] = String.format("%d_%d_%d_aaa", i, i, i).getBytes();
      c2.length[i] = 9;
      c3.vector[i] = String.format("bbb_%d", i).getBytes();
      c3.length[i] = 5;
    }
    batch.size = 3;

    OrcFile.WriterOptions options = OrcFile.writerOptions(new Configuration()).setSchema(schema);
    Writer writer;

    Map<CompressionKind, String> maps = ImmutableMap.of(
            CompressionKind.NONE, "no_compress",
            CompressionKind.LZ4, "compress_lz4",
            CompressionKind.LZO, "compress_lzo",
            CompressionKind.SNAPPY, "compress_snappy",
            CompressionKind.ZLIB, "compress_zlib"
    );
    for (CompressionKind kind : maps.keySet()) {
      options.compress(kind);
      writer = OrcFile.createWriter(new Path(dir + "/" + maps.get(kind) + ".orc"), options);
      writer.addRowBatch(batch);
      writer.close();
    }
  }
}
