package cn.edu.thssdb.query;

import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.ToIntBiFunction;

public interface QueryTable extends Iterator<Row> {

  @Override
  public boolean hasNext();

  @Override
  public Row next();

  public void refresh();

  public List<MetaInfo> getMetaInfo();
}
