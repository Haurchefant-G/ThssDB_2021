package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.NoSuchUserException;
import cn.edu.thssdb.exception.UserExistsException;
import cn.edu.thssdb.exception.WrongPasswordException;
import cn.edu.thssdb.type.UserPermission;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UserManager {
    HashMap<String, User> users;

    private UserManager() {
        users = new HashMap<String, User>();
        recover();
        User admin = new User(Global.ADMIN_PASSWORD, UserPermission.ADMIN);
        users.put(Global.ADMIN_USERNAME, admin);
    }

    private void recover() {
        File f = new File( ".users");
        if(!f.exists()) {
            try {
                f.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            while(line != null && !line.equals("")) {
                String[] s = line.split(" ");
                int p = Integer.parseInt(s[2]);
                UserPermission permission = (0 <= p && p <= 1) ?  UserPermission.values()[p] : UserPermission.USER;
                User user = new User(s[1], permission);
                users.put(s[0], user);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void persist() {
        try {
            FileWriter fw = new FileWriter(".users");
            BufferedWriter bw = new BufferedWriter(fw);
            Iterator iter = users.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, User> entry = (Map.Entry<String, User>) iter.next();
                String username = entry.getKey();
                String password = entry.getValue().getPassword();
                int permission = entry.getValue().getUserPermission().ordinal();
                bw.write(username + " " + password + " " + permission);
                bw.newLine();
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static UserManager getInstance() {
        return UserManager.UserManagerHolder.INSTANCE;
    }

    public boolean login(String username, String password) {
        User user = users.get(username);
        if (user == null) {
            throw new NoSuchUserException(username);
        }
        if (!user.getPassword().equals(password)) {
            throw new WrongPasswordException();
        }
        return true;
    }

    public void register(String username, String password, UserPermission userPermission) {
        if (users.containsKey(username)) {
            throw new UserExistsException(username);
        }
        User user = new User(password, userPermission);
        users.put(username, user);
        persist();
    }

    public void delete(String username) {
        if (!users.containsKey(username)) {
            throw new NoSuchUserException(username);
        }
        users.remove(username);
        persist();
    }

    public static class UserManagerHolder {
        private static final UserManager INSTANCE = new UserManager();
        private UserManagerHolder() {

        }
    }
}
