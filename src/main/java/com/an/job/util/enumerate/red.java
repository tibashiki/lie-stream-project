package com.an.job.util.enumerate;

public enum red {
    start("\u001B[31m"),
    end("\u001B[0m");
    private String value;

    red(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
