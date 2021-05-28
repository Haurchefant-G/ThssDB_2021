package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.NoSuchUserException;
import cn.edu.thssdb.exception.UserExistsException;
import cn.edu.thssdb.exception.WrongPasswordException;
import cn.edu.thssdb.type.UserPermission;

import java.util.HashMap;

public class UserManagers {
    HashMap<String, User> users;

    public User login(String username, String password) {
        User user = users.get(username);
        if (user == null) {
            throw new NoSuchUserException(username);
        }
        if (!user.getPassword().equals(password)) {
            throw new WrongPasswordException();
        }
        return user;
    }

    public void register(String username, String password, UserPermission userPermission) {
        if (users.containsKey(username)) {
            throw new UserExistsException(username);
        }
        User user = new User(password, userPermission);
        users.put(username, user);
    }
}
