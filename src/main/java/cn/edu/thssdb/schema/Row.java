package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries = null;
  public int page;
  public int offset;

  public Row() { this.entries = new ArrayList<>(); }

  public Row(int page, int offset) { this.page = page; this.offset = offset; }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null)
      return "EMPTY";
    StringJoiner sj = new StringJoiner(",");
    for (Entry e : entries)
      sj.add(e.toString());
    return sj.toString();
  }

  public void addToStringList(List<String> l) {
    for (Entry e : entries)
      l.add(e.toString());
  }

  public Entry getEntry(int index) { return entries.get(index); }

  public Row updateRow(int index, Entry valueEntry) {
    Entry[] newentries = new Entry[entries.size()];
    for (int i = 0; i < entries.size(); i++) {
      newentries[i] = new Entry(entries.get(i).value);
    }
    newentries[index] = valueEntry;
    return new Row(newentries);
  }
}
