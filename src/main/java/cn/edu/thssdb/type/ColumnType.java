package cn.edu.thssdb.type;

public enum ColumnType {
  INT,
  LONG,
  FLOAT,
  DOUBLE,
  STRING;

  public static boolean columnValueTypeCheck(ColumnType columnType, ValueType valueType) {
    if (valueType == ValueType.NULL || valueType == ValueType.COLUMN || valueType == ValueType.STRING) {
      return true;
    }

    if (columnType == ColumnType.INT || columnType == ColumnType.LONG) {
      if (valueType == ValueType.INT)  {
        return true;
      }
    } else if (columnType == ColumnType.FLOAT || columnType == ColumnType.DOUBLE) {
      if (valueType == ValueType.INT || valueType == ValueType.DOUBLE) {
        return true;
      }
    }
    return false;
  }

  ColumnType() {

  }
}
