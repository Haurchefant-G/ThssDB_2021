package cn.edu.thssdb.parser.statement;

import cn.edu.thssdb.rpc.thrift.Status;

public abstract class Statement {

    public abstract StatementType getType();
}
