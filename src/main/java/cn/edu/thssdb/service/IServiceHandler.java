package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.NoSuchUserException;
import cn.edu.thssdb.exception.WrongPasswordException;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetReq;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.parser.SQLResult;
import cn.edu.thssdb.schema.UserManager;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.Date;
import java.util.List;

public class IServiceHandler implements IService.Iface {
  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO: password check?
    try {
      UserManager.getInstance().login(req.username, req.password);
    } catch (Exception e) {
      Status status = new Status(Global.FAILURE_CODE);
      status.msg = e.getMessage();
      ConnectResp resp = new ConnectResp(status, -1);
      return resp;
    }
    Status status = new Status(Global.SUCCESS_CODE);
    long sessionId = ThssDB.getInstance().setupSession();
    ConnectResp resp = new ConnectResp(status, sessionId);
    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    // TODO
    ThssDB.getInstance().deleteSession(req.getSessionId());
    Status status = new Status(Global.SUCCESS_CODE);
    DisconnetResp resp = new DisconnetResp(status);
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    ThssDB thssDB = ThssDB.getInstance();
    List<SQLResult> results = thssDB.execute(req.getStatement(), req.sessionId);
    SQLResult result = results.get(0);
    Status status = result.isSucceed() ? new Status(Global.SUCCESS_CODE) : new Status(Global.FAILURE_CODE);
    boolean isAbort = result.isAbort();
    boolean hasResult = result.isHasResult();
    status.msg = result.getMsg();
    ExecuteStatementResp resp = new ExecuteStatementResp(status, isAbort, hasResult);
    if (result.isHasResult()) {
      resp.setColumnsList(result.getColumnList());
      resp.setRowList(result.getRowList());
    }
    return resp;
  }

  public long generateSessionId() {
    return System.currentTimeMillis();
  }
}
