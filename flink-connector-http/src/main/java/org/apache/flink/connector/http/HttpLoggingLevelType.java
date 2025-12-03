package org.apache.flink.connector.http;

/** Defines the level of http content that will be logged. */
public enum HttpLoggingLevelType {
    MIN,
    REQ_RESP,
    MAX;

    public static HttpLoggingLevelType valueOfStr(String code) {
        return code == null ? MIN : valueOf(code);
    }
}
