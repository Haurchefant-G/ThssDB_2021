package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class JointTable implements QueryTable {
    private Queue<Row> buffer = new LinkedList<>();
    private Stack<Row> rows = new Stack<>();

    private List<Table> tables;
    private List<Iterator<Row>> iterators = new ArrayList<>();
    private Predicate<Row> join;
    private List<MetaInfo> metaInfos;

    public JointTable(List<Table> tables, Where where) {
        this.tables = tables;
        for (Table table : tables) {
            iterators.add(table.iterator());
        }
        metaInfos = tables.stream().map(table -> new MetaInfo(table.getTableName(), table.getColumns())).collect(Collectors.toList());
        join = where.parse(metaInfos);
        pushNextLegalRowToBuffer();
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty();
    }

    @Override
    public Row next() {
        if (buffer.isEmpty()) {
            return null;
        }
        Row row = buffer.poll();
        pushNextLegalRowToBuffer();
        return row;
    }

    private void pushNextLegalRowToBuffer() {
        while (true) {
            Row row = getNextJoinRow();
            if (row == null) {
                return ;
            }
            if (!join.test(row)) {
                continue;
            }
            buffer.add(row);
            return ;
        }
    }

    private Row getNextJoinRow() {
        if (rows.isEmpty()) {
            // init
            for (Iterator<Row> iterator : iterators) {
                if (!iterator.hasNext()) {
                    return null;
                }
                rows.push(iterator.next());
            }
        } else {
            // cartesian product
            int index = rows.size() - 1;
            for (; index >= 0; index--) {
                rows.pop();
                if (!iterators.get(index).hasNext()) {
                    iterators.set(index, tables.get(index).iterator());
                } else {
                    break;
                }
            }
            // all value has been visited
            if (index < 0) {
                return null;
            }
            for (int i = index; i < iterators.size(); i++) {
                if (!iterators.get(i).hasNext()) {
                    return null;
                }
                rows.push(iterators.get(i).next());
            }
        }
        return QueryResult.combineRow(rows);
    }


    @Override
    public void refresh() {
        for (int i = 0; i < tables.size(); i++) {
            iterators.set(i, tables.get(i).iterator());
        }
    }

    @Override
    public List<MetaInfo> getMetaInfo() {
        return metaInfos;
    }
}
