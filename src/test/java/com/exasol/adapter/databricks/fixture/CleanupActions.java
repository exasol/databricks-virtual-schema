package com.exasol.adapter.databricks.fixture;

import java.util.ArrayList;
import java.util.List;

public class CleanupActions {
    private final List<CleanupAction> cleanupActions = new ArrayList<>();

    public void add(final String name, final Runnable action) {
        this.cleanupActions.add(new CleanupAction(name, action));
    }

    public void cleanup() {
        this.cleanupActions.forEach(CleanupAction::run);
        this.cleanupActions.clear();
    }

    private record CleanupAction(String name, Runnable action) implements Runnable {
        @Override
        public void run() {
            try {
                this.action().run();
            } catch (final Exception e) {
                throw new IllegalStateException("Cleanup action '" + this.name() + "' failed: " + e.getMessage(), e);
            }
        }
    }
}
