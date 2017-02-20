package cn.edu.pku.hql.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.apache.orc.Reader;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Created by quanlong.huang on 1/6/17.
 */
public class BasicUsage {

  public static void main(String[] args) throws IOException, DataFormatException {
    String dir = "/Users/quanlong.huang/Downloads";
    String filePath = dir + "/plain_orc.orc";
    //String filePath = dir + "/empty.orc";
    Reader reader = OrcFile.createReader(new Path(filePath),
            OrcFile.readerOptions(new Configuration()));
    OrcProto.FileTail tail = reader.getFileTail();

    OrcProto.PostScript ps = tail.getPostscript();
    System.out.println("Compression: " + ps.getCompression());
    System.out.println("CompressionBlockSize: " + ps.getCompressionBlockSize());
    System.out.println("Magic: " + ps.getMagic());
    System.out.println("FooterLength: " + ps.getFooterLength());
    System.out.println("MetadataLength: " + ps.getMetadataLength());

    OrcProto.Footer footer = tail.getFooter();
    System.out.println("FooterMetaDataCount: " + footer.getMetadataCount());
    System.out.println("FooterContentLength: " + footer.getContentLength());

    InputStream in = new FileInputStream(dir + "/plain.footer.zlib");
    byte[] buffer = new byte[133];
    in.read(buffer);
    Inflater decompresser = new Inflater(true);
    decompresser.setInput(buffer);
    byte[] result = new byte[149];
    int resultLength = decompresser.inflate(result);
    decompresser.end();
    System.out.println("resultLength = " + resultLength);
    System.out.println(Arrays.toString(result));
    System.out.println(result[148]);
    System.out.println(OrcProto.Footer.parseFrom(result));
    System.out.println(new String(result));
    for (int i = 0; i < resultLength; i++) {
      System.out.print(String.format(",0x%02x", result[i]));
      if (i % 16 == 15)  System.out.println();
    }
//
//    byte[] res = new byte[resultLength];
//    for (int i = 0; i < resultLength; i++)  res[i] = result[i];
//    //System.out.println(OrcProto.Footer.parseFrom(res));
//    FileWriter out = new FileWriter(dir + "/decompressed.footer");
//    for (byte b : res) {
//      out.write(b);
//    }
//    out.close();
  }

  public static void main0(String[] args) throws IOException {
    String filePath = "/Users/quanlong.huang/workspace/orc/java/core/src/test/resources/orc-file-11-format.orc";
    Reader reader = OrcFile.createReader(new Path(filePath),
            OrcFile.readerOptions(new Configuration()));
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    List<TypeDescription> colTypes = reader.getSchema().getChildren();
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        for (int c = 0; c < batch.cols.length; ++c) {
          if (batch.cols[c] instanceof LongColumnVector) {
            LongColumnVector lc = (LongColumnVector) batch.cols[c];
            System.out.println(String.format("r=%d, c=%d, value=%d",
                    r, c, lc.vector[r]));
          }
        }
      }
    }
    rows.close();
  }
}
