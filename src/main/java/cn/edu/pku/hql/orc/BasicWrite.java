package cn.edu.pku.hql.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Create a ORC file with following contents:
 *  {id: 0, array_col: NULL }
 *  {id: 1, array_col: [] }
 *  {id: 2, array_col: [NULL] }
 *  {id: 3, array_col: [[]] }
 *  {id: 4, array_col: [[NULL]] }
 *  {id: 5, array_col: [[1, NULL], [2]] }
 *  {id: 6, array_col: [[3]] }
 *
 * Data layout in the stripe:
 * |<-- id data -->|<-- outer array metadata -->|<-- inner array metadata -->|<-- inner array data -->|
 *
 */
public class BasicWrite {
  public static void main(String[] args) throws IOException {
    String dir = "/Users/quanlong.huang/Downloads";
    TypeDescription schema = TypeDescription.fromString("struct<id:bigint,array_col:array<array<int>>>");
    Writer writer = OrcFile.createWriter(new Path(dir + "/array_tbl.orc"),
            OrcFile.writerOptions(new Configuration()).setSchema(schema));

    VectorizedRowBatch batch = schema.createRowBatch(100);
    LongColumnVector id = (LongColumnVector) batch.cols[0];
    ListColumnVector outer_array = (ListColumnVector) batch.cols[1];
    ListColumnVector inner_array = (ListColumnVector) outer_array.child;
    LongColumnVector inner_item = (LongColumnVector) inner_array.child;

    outer_array.noNulls = false;
    inner_array.noNulls = false;
    inner_item.noNulls = false;

    int row = 0;
    int outer_offset = 0;
    int inner_offset = 0;

    // 0, null
    id.vector[row] = row;
    outer_array.isNull[row] = true;

    // 1, array()
    id.vector[++row] = row;
    outer_array.isNull[row] = false;
    outer_array.offsets[row] = outer_offset;
    outer_array.lengths[row] = 0;

    // 2, array(null)
    id.vector[++row] = row;
    outer_array.isNull[row] = false;
    outer_array.offsets[row] = outer_offset;
    outer_array.lengths[row] = 1;
    inner_array.isNull[outer_offset] = true;
    outer_offset++;

    // 3, array(array())
    id.vector[++row] = row;
    outer_array.isNull[row] = false;
    outer_array.offsets[row] = outer_offset;
    outer_array.lengths[row] = 1;
    inner_array.isNull[outer_offset] = false;
    inner_array.offsets[outer_offset] = inner_offset;
    inner_array.lengths[outer_offset] = 0;
    outer_offset++;

    // 4, array(array(null))
    id.vector[++row] = row;
    outer_array.isNull[row] = false;
    outer_array.offsets[row] = outer_offset;
    outer_array.lengths[row] = 1;
    inner_array.isNull[outer_offset] = false;
    inner_array.offsets[outer_offset] = inner_offset;
    inner_array.lengths[outer_offset] = 1;
    inner_item.isNull[inner_offset] = true;
    outer_offset++;
    inner_offset++;

    // 5, array(array(1,null), array(2))
    id.vector[++row] = row;
    outer_array.isNull[row] = false;
    outer_array.offsets[row] = outer_offset;
    outer_array.lengths[row] = 2;
    inner_array.isNull[outer_offset] = false;
    inner_array.isNull[outer_offset + 1] = false;
    inner_array.offsets[outer_offset] = inner_offset;
    inner_array.lengths[outer_offset] = 2;
    inner_array.offsets[outer_offset + 1] = inner_offset + 2;
    inner_array.lengths[outer_offset + 1] = 1;
    inner_item.vector[inner_offset] = 1;
    inner_item.isNull[inner_offset + 1] = true;
    inner_item.vector[inner_offset + 2] = 2;
    outer_offset += 2;
    inner_offset += 3;

    // 6, array(array(3))
    // isNull defaults to false, no need to set this.
    id.vector[++row] = row;
    outer_array.offsets[row] = outer_offset;
    outer_array.lengths[row] = 1;
    inner_array.offsets[outer_offset] = inner_offset;
    inner_array.lengths[outer_offset] = 1;
    inner_item.vector[inner_offset] = 3;

    batch.size = ++row;
    writer.addRowBatch(batch);
    writer.close();

  }
}
