package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.UserPermission;


public class User {

    private String password;
    private UserPermission userPermission;

    User(String password, UserPermission userPermission) {
        this.password = password;
        this.userPermission = userPermission;
    }

    String getPassword() {
        return password;
    }

    public UserPermission getUserPermission() {
        return userPermission;
    }
}
