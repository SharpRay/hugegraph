package com.mininglamp.hugegraph.backend.store.clickhouse;

public class ClickhouseTables {

    public static final String BOOLEAN = "UInt8";
    public static final String TINYINT = "Int8";
    public static final String INT = "Int32";
    public static final String BIGINT = "Int64";
    public static final String NUMERIC = "Float64";
    public static final String SMALL_TEXT = "String";
    public static final String MID_TEXT = "String";
    public static final String LARGE_TEXT = "String";
    public static final String HUGE_TEXT = "String";

    private static final String DATATYPE_PK = INT;
    private static final String DATATYPE_SL = INT;  // VL/EL
    private static final String DATATYPE_IL = INT;

    private static final String SMALL_JSON = MID_TEXT;
    private static final String LARGE_JSON = LARGE_TEXT;

}
