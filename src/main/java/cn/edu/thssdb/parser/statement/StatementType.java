package cn.edu.thssdb.parser.statement;

public enum StatementType {
    CREATE_DATABASE,
    DROP_DATABASE,
    SHOW_DATABASE,
    USE_DATABASE,

    CREATE_TABLE,
    SHOW_TABLES,
    DROP_TABLE,
    SHOW_TABLE_META,

    CREATE_USER,
    DROP_USER,

    INSERT,
    SELECT,
    UPDATE,
    DELETE,

    BEGIN_TRANSACTION,
    COMMIT,
}
