package com.nosqldriver.sql;

import java.util.EventListener;

public interface StatementEventListener extends EventListener {
    void executed(StatementEvent event);
    void updated(StatementEvent event);
    void queried(StatementEvent event);
}
