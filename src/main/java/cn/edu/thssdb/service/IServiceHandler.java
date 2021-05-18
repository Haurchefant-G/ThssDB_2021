package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.ServerSQLVisitor;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;

import java.util.Date;

public class IServiceHandler implements IService.Iface {

  ServerSQLVisitor ServerSql = new ServerSQLVisitor();

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    Status status = new Status(Global.SUCCESS_CODE);
    ConnectResp resp = new ConnectResp(status, generateSessionId());
    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetResp req) throws TException {
    // TODO
    return null;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    // TODO
    //ExecuteStatementResp resp = new ExecuteStatementResp();
    CharStream stream = CharStreams.fromString(req.getStatement());
    SQLLexer lexer = new SQLLexer(stream);
    CommonTokenStream token = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(token);
    //SQLParser.ParseContext c = parser.parse();
//        ServerSql.visitParse(parser.parse());
    return (ExecuteStatementResp) ServerSql.visitSql_stmt_list(parser.sql_stmt_list());
  }

  public long generateSessionId() {
    return System.currentTimeMillis();
  }
}
