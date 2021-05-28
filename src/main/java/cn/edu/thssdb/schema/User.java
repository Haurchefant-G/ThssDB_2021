package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.UserPermission;


public class User {

    public String password;
    public UserPermission userPermission;

    public User(String password, UserPermission userPermission) {
        this.password = password;
        this.userPermission = userPermission;
    }

    public String getPassword() {
        return password;
    }

    public UserPermission getUserPermission() {
        return userPermission;
    }
}
