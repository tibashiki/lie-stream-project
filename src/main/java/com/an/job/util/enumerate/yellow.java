package com.an.job.util.enumerate;

public enum  yellow {
    start("\u001B[33m"),
    end("\u001B[0m");
    private String value;

    yellow(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
