package cn.edu.pku.hql.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;

/**
 * Created by quanlong.huang on 1/13/17.
 */
public class BasicWrite {
  public static void main(String[] args) throws IOException {
    String dir = "/Users/quanlong.huang/Downloads";
    TypeDescription schema = TypeDescription.fromString("int,string,string");
    Writer writer = OrcFile.createWriter(new Path(dir + "/empty2.orc"),
            OrcFile.writerOptions(new Configuration()).setSchema(schema));
    writer.close();
  }
}
