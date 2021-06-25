package cn.edu.thssdb.query;
import cn.edu.thssdb.schema.Row;

import java.util.Iterator;
import java.util.List;

public interface QueryTable extends Iterator<Row> {

  @Override
  public boolean hasNext();

  @Override
  public Row next();

  public void refresh();

  public List<MetaInfo> getMetaInfo();
}
