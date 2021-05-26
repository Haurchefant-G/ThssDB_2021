package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JointTable extends QueryTable implements Iterator<Row> {
    private List<Table> tables;
    private Predicate<Row> condition;
    private List<MetaInfo> metaInfos;

    public JointTable(List<Table> tables, Where condition) {
        super();
        this.tables = tables;

        metaInfos = tables.stream().map(table -> new MetaInfo(table.getTableName(), table.columns)).collect(Collectors.toList());
        this.condition = condition.parse(metaInfos);
    }

}
