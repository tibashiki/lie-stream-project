package com.an.job.util.enumerate;

public enum blue {
    start("\u001B[34m"),
    end("\u001B[0m");
    private String value;

    blue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
