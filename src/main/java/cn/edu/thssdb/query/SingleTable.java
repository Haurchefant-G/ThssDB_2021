package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SingleTable implements QueryTable {
    private Table table;
    private Iterator<Row> iterator;
    private MetaInfo metaInfo;

    public SingleTable(Table table) {
        this.table = table;
        this.iterator = table.iterator();
        metaInfo = new MetaInfo(table.getTableName(), table.columns);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Row next() {
        return iterator.next();
    }

    @Override
    public void refresh() {
        iterator = table.iterator();
    }

    @Override
    public List<MetaInfo> getMetaInfo() {
        return new ArrayList<MetaInfo>() {{
            add(metaInfo);
        }};
    }
}
