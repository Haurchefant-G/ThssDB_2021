package cn.edu.thssdb.schema;

import java.util.ArrayList;
import java.util.HashMap;

public class TransactionManager {
    private HashMap<Long, ArrayList<String>> S_lock_dic;        // 所有session的S锁
    private HashMap<Long, ArrayList<String>> X_lock_dic;        // 所有session的X锁
    private ArrayList<Long> transaction_sessions;               // transaction中的session
    private ArrayList<Long> queue_sessions;                     // 阻塞的session

    public TransactionManager() {
        S_lock_dic = new HashMap<>();
        X_lock_dic = new HashMap<>();
        transaction_sessions = new ArrayList<Long>();
        queue_sessions = new ArrayList<Long>();
    }

    public int get_session_size() {
        return transaction_sessions.size();
    }

    public boolean contain_session(long session_id) {
        return transaction_sessions.contains(session_id);
    }

    public void add_session(long session_id) {
        transaction_sessions.add(session_id);
    }

    public void remove_session(long session_id) {
        transaction_sessions.remove(session_id);
    }

    public void put_X_lock(long session_id, ArrayList<String> X_lock) {
        X_lock_dic.put(session_id, X_lock);
    }

    public ArrayList<String> get_X_lock(long session_id) {
        return X_lock_dic.get(session_id);
    }

    public void put_S_lock(long session_id, ArrayList<String> S_lock) {
        S_lock_dic.put(session_id, S_lock);
    }

    public ArrayList<String> get_S_lock(long session_id) {
        return S_lock_dic.get(session_id);
    }

    public void wait_for_write(long session_id, Table table) {
        while(true)
        {
            if(!queue_sessions.contains(session_id))
            {
                int res = table.add_X_lock(session_id);
                if(res == -1)
                    queue_sessions.add(session_id);
                else
                {
                    if(res == 1)
                    {
                        ArrayList<String> tmp = X_lock_dic.get(session_id);
                        tmp.add(table.getTableName());
                        X_lock_dic.put(session_id,tmp);
                    }
                    break;
                }
            }
            else
            {
                if(queue_sessions.get(0).equals(session_id))
                {
                    int res = table.add_X_lock(session_id);
                    if(res != -1)
                    {
                        if(res == 1)
                        {
                            ArrayList<String> tmp = X_lock_dic.get(session_id);
                            tmp.add(table.getTableName());
                            X_lock_dic.put(session_id, tmp);
                        }
                        queue_sessions.remove(0);
                        break;
                    }
                }
            }

            try
            {
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println("sleep error");
            }
        }
    }

    public void wait_for_read(long session_id, ArrayList<Table> tables) {
        while (true) {
            if (!queue_sessions.contains(session_id))
            {
                ArrayList<Integer> res = new ArrayList<>();
                for (Table table : tables)
                    res.add(table.add_S_lock(session_id));
                if (res.contains(-1))
                {
                    for (Table table : tables)
                        table.free_S_lock(session_id);

                    queue_sessions.add(session_id);
                }
                else
                    break;

            }
            else
            {
                if (queue_sessions.get(0).equals(session_id))
                {
                    ArrayList<Integer> res = new ArrayList<>();
                    for (Table table : tables)
                        res.add(table.add_S_lock(session_id));

                    if (res.contains(-1)) {
                        for (Table table : tables)
                            table.free_S_lock(session_id);
                    }
                    else {
                        queue_sessions.remove(0);
                        break;
                    }
                }
            }

            try
            {
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println("sleep error");
            }
        }
    }
}