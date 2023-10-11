package com.an.job.util.enumerate;

public enum green {
    start("\u001B[32m"),
    end("\u001B[0m");
    private String value;

    green(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
