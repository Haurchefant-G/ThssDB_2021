package cn.edu.thssdb.utils;

import cn.edu.thssdb.exception.FileNotExistAndCannotCreate;
import cn.edu.thssdb.exception.RowNotExistException;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.ColumnType;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class FileStorage {
    /** 表头Schema */
    private ArrayList<Column> columns;
    /** 每行记录字节数，为各column的maxlenghth之和+1 */
    private int rowLen = 0;
    //private int maxRow = 0;
    /** 表对应文件名 */
    private String filename = null;
    /** 维护的页存储的最大数量 */
    private static int keeppages = 1024;
    /** 内存中的页存 */
    private ArrayList<Page> pages;
    /** 在内存中的页码，与pages中顺序保持一致 */
    private ArrayList<Integer> pageIndex;

    /** 在内存中的页的比特数 */
    static private int PAGE_BYTES = 4 * 1024;

    /** 存储文件读写用 */
    RowRandomAccessFile file = null;

    protected class RowRandomAccessFile extends RandomAccessFile {
        private ArrayList<Column> columns;
        public RowRandomAccessFile(String filename, String mode, ArrayList<Column> columns) throws FileNotFoundException {
            super(filename, mode);
            this.columns = columns;
        }

        public void writeRow(Row row) throws IOException {
            ArrayList<Entry> entries = row.getEntries();
            writeEntryValue(entries.get(0).value, columns.get(0).getType());
            for(int i = 1; i < columns.size(); ++i) {
                //write(',');
                writeEntryValue(entries.get(i).value, columns.get(i).getType());
            }
            write('\n');
        }

        public void writeEntryValue(Comparable value, ColumnType type) throws IOException {
            switch (type) {
                case INT:
                    this.writeInt((Integer) value);
                    break;
                case LONG:
                    this.writeLong((Long) value);
                    break;
                case FLOAT:
                    this.writeFloat((Float) value);
                    break;
                case DOUBLE:
                    this.writeDouble((Double) value);
                    break;
                case STRING:
                    this.writeUTF((String) value);
                    break;
            }
        }
    }

    /**
     * 构造某个表相应文件存储的管理
     * @param filename 表对应文件名，如果不存在，在插入时会自动创建
     * @param columns 表头schema
     */
    public FileStorage(String filename, ArrayList<Column> columns) {
        rowLen = 2; // 每行末尾的换行符预留位置
        rowLen += 4; // 页式存储中每行前用于指示下一个空行的index
        for (Column column : this.columns = columns) {
            rowLen += column.getMaxLength();
            if (column.getType() == ColumnType.STRING) {
                rowLen += 4;
            }
        }
        this.filename = filename;
        pageIndex = new ArrayList<>();
        pages = new ArrayList<>();
    }

    /**
     * 将一行Row转化为一个ByteBuffer，便于写入到内存中的页存储中
     * @param row 要写入的Row
     * @return 转化后的ByteBuffer
     */
    public ByteBuffer RowToByteBuffer(Row row) {
        ArrayList<Entry> entries = row.getEntries();
        ByteBuffer rowbuffer = ByteBuffer.allocate(rowLen);

        rowbuffer.putInt(0);

        for(int i = 0; i < columns.size(); ++i) {
            writeColumntoBuffer(rowbuffer, entries.get(i).value, columns.get(i).getType());
        }
        rowbuffer.putChar(rowLen - 2, '\n');
        return rowbuffer;
    }

    /**
     * 将一行某栏的数据写入到ByteBuffer
     * @param rowbuffer 写入的ByteBuffer
     * @param value 待写入的数据
     * @param type 待写入的数据的类型
     */
    public void writeColumntoBuffer(ByteBuffer rowbuffer, Comparable value, ColumnType type) {
        switch (type) {
            case INT:
                rowbuffer.putInt((Integer) value);
                break;
            case LONG:
                rowbuffer.putLong((Long) value);
                break;
            case FLOAT:
                rowbuffer.putFloat((Float) value);
                break;
            case DOUBLE:
                rowbuffer.putDouble((Double) value);
                break;
            case STRING:
                rowbuffer.putInt(((String) value).length());
                rowbuffer.put(((String) value).getBytes());
                break;
        }
    }

    public Row ByteBufferToRow(ByteBuffer rowbuffer) {
        Entry []entries = new Entry[columns.size()];
        rowbuffer.rewind();
        rowbuffer.getInt();
        for(int i = 0; i < columns.size(); ++i) {
            entries[i] = readColumntoEntry(rowbuffer, columns.get(i).getType());
        }
        return new Row(entries);
    }

    public Entry readColumntoEntry(ByteBuffer rowbuffer, ColumnType type) {
        switch (type) {
            case INT:
                return new Entry(rowbuffer.getInt());
            case LONG:
                return new Entry(rowbuffer.getLong());
            case FLOAT:
                return new Entry(rowbuffer.getFloat());
            case DOUBLE:
                return new Entry(rowbuffer.getDouble());
            case STRING:
            default:
                int len = rowbuffer.getInt();
                byte []str = new byte[len];
                rowbuffer.get(str);
                return new Entry(new String(str));
        }
    }


//    // TODO 未完成
//    /**
//     * 直接往存储文件中写入
//     * @param row
//     * @throws IOException
//     */
//    public void insert(Row row) throws IOException {
//        for (Iterator<Page> it = pages.iterator(); it.hasNext(); ) {
//            Page p = it.next();
//            int empty = p.hasEmptyRow();
//            if(empty != 0) {
//                ByteBuffer rowbuffer = RowToByteByffer(row);
//                p.writeRow(empty, rowbuffer);
//                return;
//            }
//        }
//        try {
//            openFile();
//        } catch (FileNotFoundException e) {
//
//        }
//        //file.write();
//        int page = 0;
//        for (; page * PAGE_BYTES < file.length(); ++page) {
//            int pointer = page * PAGE_BYTES;
//            file.seek(pointer);
//            int head = file.readInt();
//            if (head == 0) {
//                continue;
//            }
//            int rowpointer = pointer + head;
//            file.seek(rowpointer);
//            head = file.readInt();
//            file.seek(pointer);
//            file.writeInt(head);
//            return;
//        }
//        file.setLength(file.length() + PAGE_BYTES);
//        file.seek(0);
//    }

    /**
     * 在内存存储页中插入某行，如果内存存储页中无位置可插入，会新取一页到内存中
     * @param row
     * @throws IOException
     */
    public void insertToPage(Row row) throws IOException {
        for (Iterator<Page> it = pages.iterator(); it.hasNext(); ) {
            Page p = it.next();
            int empty = p.hasEmptyRow();
            if(empty != 0) {
                ByteBuffer rowbuffer = RowToByteBuffer(row);
                p.writeRow(empty, rowbuffer);
                return;
            }
        }
        try {
            openFile();
        } catch (FileNotExistAndCannotCreate e) {

        }
        int page = 0;
        boolean emptypage = false;
        int head = 0;
        for (; page * PAGE_BYTES < file.length(); ++page) {
            int pointer = page * PAGE_BYTES;
            file.seek(pointer);
            head = file.readInt();
            if (head != 0) {
                emptypage = true;
                break;
            }

        }
        if (!emptypage) {
            file.setLength(file.length() + PAGE_BYTES);
            head = rowLen;
        }
        Page p = fetchPage(page);
        ByteBuffer rowbuffer = RowToByteBuffer(row);
        p.writeRow(head, rowbuffer);
        row.page = page;
        row.offset = head;
        return;
    }

//    // TODO 未完成
//    /**
//     * 直接从存储文件中删除，需更新内存页
//     * @param page
//     * @param offset
//     * @throws IOException
//     */
//    public void delete(int page, int offset) throws IOException {
//        try {
//            openFile();
//        } catch (FileNotFoundException e) {
//
//        }
//        file.seek(page * PAGE_BYTES);
//    }

    /**
     * 从内存存储页中删除某行，如果该行所在页不在内存中，会将该页取到内存中
     * @param page 页码
     * @param offset 该行在页中起始字节
     * @throws IOException
     */
    public void deleteFromPage(int page, int offset) throws IOException {
        int index = pageIndex.indexOf(page);
        Page p = null;
        if (index >= 0) {
            p = pages.get(index);
        } else {
            p = fetchPage(page);
        }
        p.deleteRow(offset);
    }

//    public void deleteAll() throws IOException {
//        clearAll();
//    }

    /**
     * 在内存存储页中修改某行，先删除原先行，再插入修改后的行
     * @param page 页码
     * @param offset 该行在页中起始字节
     * @param newrow 修改后该行内容
     * @throws IOException
     */
    public void updateRowInPage(int page, int offset, Row newrow) throws IOException {
        deleteFromPage(page, offset);
        insertToPage(newrow);
    }

    /**
     * 在内存存储页中查询某行记录并返回存储记录的bytebuffer，如果该行所在页不在内存中，会将该页取到内存中
     * @param page 页码
     * @param offset 该行在页中起始字节
     * @return 存储查询行记录的bytebuffer
     * @throws IOException
     */
    public Row searchRowInPage(int page, int offset) throws IOException {
        int index = pageIndex.indexOf(page);
        Page p = null;
        if (index >= 0) {
            p = pages.get(index);
        } else {
            openFile();
            if (file.length() < page * PAGE_BYTES + offset + rowLen)
                throw new RowNotExistException();
            p = fetchPage(page);
        }
        ByteBuffer buffer = p.searchRow(offset);
        Row row = ByteBufferToRow(buffer);
        row.page = page;
        row.offset = offset;
        return row;
    }

    /**
     * 将新的一页数据提取到内存中
     * @param page 页码
     * @return 新取来的页
     * @throws IOException
     */
    public Page fetchPage(int page) throws IOException {
        if (pages.size() >= keeppages) {
            clearUp();
        }
        byte[] content = new byte[PAGE_BYTES];
        try {
            openFile();
        } catch (FileNotExistAndCannotCreate e) {

        }
        file.seek(page * PAGE_BYTES);
        int readLen = file.read(content);
        if (readLen < PAGE_BYTES) {
            file.seek(page * PAGE_BYTES);
            file.write(content);
        }
        Page newpage = new Page(page, content, rowLen);
        pageIndex.add(page);
        pages.add(newpage);
        return newpage;
    }

    /**
     * 删除内存中所有页中访问次数较少的一半
     */
    public void clearUp() throws IOException {
        pages.sort(new Comparator<Page>() {
            @Override
            public int compare(Page o1, Page o2) {
                return Integer.compare(o1.accessnum, o2.accessnum);
            }
        });
        for (int i = 0; i < keeppages / 2; ++i) {
            Page p = pages.remove(0);
            updateFile(p);
            p = null;
        }
        pageIndex.clear();
        for (Iterator<Page> it = pages.iterator(); it.hasNext();) {
            Page p = it.next();
            p.resetAccess();
            pageIndex.add(p.pagenum);
        }
    }

    /**
     * 清空内存中所有页
     * @throws IOException
     */
    public void clearAll() throws IOException {
        for(int i = 0, size = pages.size(); i < size; ++i) {
            Page p = pages.remove(0);
            updateFile(p);
            p = null;
        }
    }

    /**
     * 将内存中的存储页更新到存储文件中
     * @param p
     */
    void updateFile(Page p) throws IOException {
        if (p.changed) {
            file.seek(p.pagenum * PAGE_BYTES);
            file.write(p.content.array());
        }
    }

    /**
     * 打开对应存储文件，如果不存在会新建
     * @throws FileNotExistAndCannotCreate
     */
    public void openFile() throws FileNotExistAndCannotCreate {
        if (file == null) {
            try {
                File f = new File(filename);
                if (!f.exists())
                    f.createNewFile();
                file = new RowRandomAccessFile(filename, "rw", columns);
            } catch (Exception e) {
                throw new FileNotExistAndCannotCreate();
            }
        }
    }

    /**
     * 保存关闭存储文件
     * @throws IOException
     */
    public void closeFile() throws IOException {
        if (file != null) {
            for(Page p:pages) {
                updateFile(p);
            }
            file.close();
            file = null;
        }
    }

    /**
     * 对应表被关闭，保存关闭存储文件并清空内存页
     */
    public void close() {
        try {
            if (file != null) {
                clearAll();
                pages = null;
                pageIndex = null;
                columns = null;
                file.close();
                file = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public int getPageNum() {
        return pages != null ? pages.size() : 0;
    }

    public int getPageIndex(int index) {
        return pageIndex != null ? pageIndex.get(index) : -1;
    }

    public int getRowLen() {
        return rowLen;
    }

    protected int searchRowIndexInPage(int page, int offset) throws IOException {
        int index = pageIndex.indexOf(page);
        Page p = null;
        if (index >= 0) {
            p = pages.get(index);
        } else {
            openFile();
            if (file.length() < page * PAGE_BYTES + offset + rowLen)
                throw new RowNotExistException();
            p = fetchPage(page);
        }
        ByteBuffer buffer = p.searchRow(offset);
        buffer.rewind();
        return buffer.getInt();
    }

}
