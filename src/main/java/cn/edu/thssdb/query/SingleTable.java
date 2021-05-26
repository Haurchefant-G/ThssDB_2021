package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.Iterator;

public class SingleTable extends QueryTable implements Iterator<Row> {
    public SingleTable(Table table) {
        super(table);
    }

}
