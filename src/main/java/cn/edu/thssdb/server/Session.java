package cn.edu.thssdb.server;

import cn.edu.thssdb.schema.Database;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Session {
    long sessionId;
    boolean autoCommit;
    Database database;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Session(long sessionId) {
        this.sessionId = sessionId;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Database getDatabase() {
        return database;
    }

    public String getDatabaseName() {
        if (database == null) {
            return null;
        }
        return database.getName();
    }

    public void setDatabase(Database database) {
        database.start();
        this.database = database;
    }

    public void destroy() {
        if (database != null) {
            database.logoutSession();
            database.quit();
            database = null;
        }
    }

    public ReentrantReadWriteLock.WriteLock getWriteLock() {
        return lock.writeLock();
    }

    public ReentrantReadWriteLock.ReadLock getReadLock() {
        return lock.readLock();
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public void setLock(ReentrantReadWriteLock lock) {
        this.lock = lock;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        destroy();
    }
}
