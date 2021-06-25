package cn.edu.thssdb.utils;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.nio.ByteBuffer;
import java.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PageTest {
    private static Page page;
    private static int pagenum = 0;
    private static int pagesize = 4096;
    private static int rowsize = 32;
    static List<Integer> deleteIndex;

    @BeforeClass
    public static void setup() {
        page = new Page(pagenum, pagesize, rowsize);
        deleteIndex = new ArrayList<Integer>();
        for (int i = rowsize; i + rowsize <= pagesize; i += rowsize)
        {
            deleteIndex.add(i);
        }
        Collections.shuffle(deleteIndex);
    }

    @Test
    public void test1_hasEmptyRow() {
        int head = page.hasEmptyRow();
        assert head == rowsize;
    }

    @Test
    public void test2_writeRow() {
        int head = rowsize, i;
        for (i = 1; head != 0; ++i)
        {
            assert head == i * rowsize;
            ByteBuffer buf = ByteBuffer.allocate(rowsize);
            buf.putInt(i * 4);
            page.writeRow(head, buf);
            head = page.hasEmptyRow();
        }
        assert head == 0;
        assert (i + 1) * rowsize > pagesize;
    }

    @Test
    public void test3_searchRowAfterWrite() {
        int head = rowsize, i;
        for (i = 1; head + rowsize <= pagesize; ++i)
        {
            ByteBuffer buf = page.searchRow(head);
            int j = buf.getInt();
            assert i * 4 == j;
            head += rowsize;
        }
        assert (i + 1) * rowsize > pagesize;
    }

    @Test
    public void test4_deleteRow() {
        int head = 0;
        for(Iterator<Integer> li = deleteIndex.iterator(); li.hasNext();)
        {
            head = li.next();
            page.deleteRow(head);
            assert head == page.hasEmptyRow();
        }
    }

    @Test
    public void test5_writeRowAfterDelete() {
        int head;
        ByteBuffer buf = ByteBuffer.allocate(rowsize);
        ListIterator<Integer> li = deleteIndex.listIterator(deleteIndex.size());
        for(; li.hasPrevious();)
        {
            head = li.previous();
            assert head == page.hasEmptyRow();
            page.writeRow(head, buf);
        }
        assert page.hasEmptyRow() == 0;
    }
}

