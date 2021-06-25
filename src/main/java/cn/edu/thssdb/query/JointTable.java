package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.*;
import java.util.function.Predicate;

public class JointTable implements QueryTable {
    private Queue<Row> buffer = new LinkedList<>();
    private Stack<Row> rows = new Stack<>();
    private Table tableLeft;
    private Table tableRight;
    private Iterator<Row> iterLeft;
    private Iterator<Row> iterRight;
    private Predicate<Row> join;
    private List<MetaInfo> metaInfos;
    private Set<Integer> leftFilledRowHash = new HashSet<>();
    private Set<Integer> rightFilledRowHash = new HashSet<>();
    private boolean outerLeft;
    private boolean outerRight;

    public JointTable(Table tableLeft, Table tableRight, Where where, boolean outerLeft, boolean outerRight) {
        this.tableLeft = tableLeft;
        this.tableRight = tableRight;
        this.outerLeft = outerLeft;
        this.outerRight = outerRight;
        this.iterLeft = tableLeft.iterator();
        this.iterRight = tableRight.iterator();
        this.metaInfos = Arrays.asList(new MetaInfo(tableLeft.getTableName(), tableLeft.columns), new MetaInfo(tableRight.getTableName(), tableRight.columns));
        this.join = where.parse(metaInfos);
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
        if (!iterLeft.hasNext() && !iterRight.hasNext()) {
            if (outerLeft) {
                pushLeftOuterRowToBuffer();
                outerLeft = false;
            }
            if (outerRight) {
                pushRightOuterRowToBuffer();
                outerRight = false;
            }
        }
        return row;
    }

    private void pushLeftOuterRowToBuffer() {
        ArrayList<Entry> entries = new ArrayList<>(Collections.nCopies(tableRight.columns.size(), null));
        Row emptyRow = new Row(entries);

        for (Row rowLeft : tableLeft) {
            if (leftFilledRowHash.contains(rowLeft.hashCode())) {
                continue;
            }
            ArrayList<Row> combinedRow = new ArrayList<>();
            combinedRow.add(rowLeft);
            combinedRow.add(emptyRow);
            buffer.add(QueryResult.combineRow(combinedRow));
        }
    }

    private void pushRightOuterRowToBuffer() {
        ArrayList<Entry> entries = new ArrayList<>(Collections.nCopies(tableLeft.columns.size(), null));
        Row emptyRow = new Row(entries);

        for (Row rowRight : tableRight) {
            if (rightFilledRowHash.contains(rowRight.hashCode())) {
                continue;
            }
            ArrayList<Row> combinedRow = new ArrayList<>();
            combinedRow.add(emptyRow);
            combinedRow.add(rowRight);
            buffer.add(QueryResult.combineRow(combinedRow));
        }
    }

    private void pushNextLegalRowToBuffer() {
        while (true) {
            Row row = getNextJoinRow();
            if (row == null) {
                return;
            }
            if (!join.test(row)) {
                continue;
            }
            leftFilledRowHash.add(rows.get(0).hashCode());
            rightFilledRowHash.add(rows.get(1).hashCode());
            buffer.add(row);
            return ;
        }
    }

    private Row getNextJoinRow() {
        if (rows.isEmpty()) {
            if (!iterLeft.hasNext()) {
                return null;
            }
            rows.push(iterLeft.next());
            if (!iterRight.hasNext()) {
                return null;
            }
            rows.push(iterRight.next());
        } else {
            if (iterRight.hasNext()) {
                rows.pop();
                rows.push(iterRight.next());
            } else {
                if (iterLeft.hasNext()) {
                    rows.pop();
                    rows.pop();
                    iterRight = tableRight.iterator();
                    rows.push(iterLeft.next());
                    rows.push(iterRight.next());
                } else {
                    return null;
                }
            }
        }
        return QueryResult.combineRow(rows);
    }

    @Override
    public void refresh() {
        iterLeft = tableLeft.iterator();
        iterRight = tableRight.iterator();
    }

    @Override
    public List<MetaInfo> getMetaInfo() {
        return metaInfos;
    }
}
