package cn.edu.thssdb.utils;

import java.nio.ByteBuffer;

/**
 * Page为一个内存中的存储页
 * 第一行记录一个数值head，记录一个可插入数据的起始字节位置
 */
public class Page {
    public int pagenum = 0;             /** 页码 */
    public int accessnum = 1;           /** 页面访问次数 */
    public boolean changed = false;     /** 页面是否修改过 */
    public ByteBuffer content;          /** 页面数据 */
    public int rowsize = 1;             /** 每行记录字节数 */

    /**
     * 根据容量创建一个新存储页
     * @param pagenum 页码
     * @param pagesize 页容量大小
     */
    public Page(int pagenum, int pagesize, int rowsize){
        this.pagenum = pagenum;
        this.content = ByteBuffer.allocate(pagesize);
        this.rowsize = rowsize;
    }

    /**
     * 根据数据创建一个新存储页
     * @param pagenum 页码
     * @param content 数据内容
     */
    public Page(int pagenum, byte[] content, int rowsize) {
        this.pagenum = pagenum;
        this.content = ByteBuffer.wrap(content);
        this.rowsize = rowsize;
    }

    /**
     * 判断是否有可写入的空行
     * @return 返回空行的起始字节，没有时返回0
     */
    public int hasEmptyRow() {
        int head = content.getInt(0);
//        if (head != 0) {
//            int empty = content.getInt(head);
//            return empty;
//        }
        return head;
    }

    /**
     * 在页中插入一行，并更新head值
     * @param index 写入起始位置
     * @param rowbuffer 写入行的数据
     */
    public void writeRow(int index, ByteBuffer rowbuffer) {
        int head = content.getInt(index);
        if (head == 0) {
            head = (index + (rowsize << 2) > content.capacity()) ? 0 : (index + rowsize);
        }
        content.putInt(0, head);
        content.position(index);
        content.put(rowbuffer.array());
        //content.reset();
        changed = true;
        ++accessnum;
    }

    /**
     * 在页中删除一行，并在删除行记录上一次删除行的起始字节位置，更新head为这次删除行的起始字节位置
     * @param index 本次删除行的起始字节位置
     */
    public void deleteRow(int index) {
        byte[] empty = new byte[rowsize];
        content.position(index);
        content.put(empty);
        int head = content.getInt(0);
        content.putInt(index, head);
        content.putInt(0, index);
        changed = true;
        //empty[rowsize-1] = '\n';
        ++accessnum;
    }

    /**
     * 在页中查询某一行，返回这一行记录
     * @param index 本次查询行的起始字节位置
     * @return 存储此行记录的bytebuffer
     */
    public ByteBuffer searchRow(int index) {
        ByteBuffer rowbuffer = ByteBuffer.allocate(rowsize);
        content.position(index);
        content.put(rowbuffer);
        ++accessnum;
        return rowbuffer;
    }

}
