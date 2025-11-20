package org.apache.flink.connector.http;

/** Defines the level of http content that will be logged. */
public enum HttpLoggingLevelType {
    MIN,
    REQRESPONSE,
    MAX;

    public static HttpLoggingLevelType valueOfStr(String code) {
        if (code == null) {
            return MIN;
        } else {
            return valueOf(code);
        }
    }
}
