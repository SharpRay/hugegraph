package com.baidu.hugegraph.backend.store.clickhouse;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

public class ClickhouseOptions extends OptionHolder {

    protected ClickhouseOptions() {
        super();
    }

    private static volatile ClickhouseOptions instance;

    public static synchronized ClickhouseOptions instance() {
        if (instance == null) {
            instance = new ClickhouseOptions();
            instance.registerOptions();
        }
        return instance;
    }


    public static final ConfigOption<String> JDBC_DRIVER =
            new ConfigOption<>(
                    "jdbc.driver",
                    "The JDBC driver class to connect database.",
                    disallowEmpty(),
                    "ru.yandex.clickhouse.ClickHouseDriver"
            );

    public static final ConfigOption<String> JDBC_URL =
            new ConfigOption<>(
                    "jdbc.url",
                    "The url of database in JDBC format",
                    disallowEmpty(),
                    "jdbc:clickhouse://127.0.0.1:8123"
            );

    public static final ConfigOption<String> JDBC_USERNAME =
            new ConfigOption<>(
                    "jdbc.username",
                    "The username to login database.",
                    disallowEmpty(),
                    "default"
            );

    public static final ConfigOption<String> JDBC_PASSWORD =
            new ConfigOption<>(
                    "jdbc.password",
                    "The password corresponding to jdbc.username",
                    null,
                    ""
            );

    public static final ConfigOption<Integer> JDBC_RECONNECT_MAX_TIMES =
            new ConfigOption<>(
                    "jdbc.reconnect_max_times",
                    "The reconnect times when the database connection fails.",
                    rangeInt(1, 10),
                    3
            );

    public static final ConfigOption<Integer> JDBC_RECONNECT_INTERVAL =
            new ConfigOption<>(
                    "jdbc.reconnect_interval",
                    "The interval(seconds) between reconnections when the " +
                            "database connection fails.",
                    rangeInt(1, 10),
                    3
            );

    public static final ConfigOption<String> JDBC_SSL_MODE =
            new ConfigOption<>(
                    "jdbc.ssl_mode",
                    "The SSL mode of connections with database.",
                    disallowEmpty(),
                    "false"
            );

//    public static final ConfigOption<String> STORAGE_ENGINE =
//            new ConfigOption<>(
//                    "jdbc.storage_engine",
//                    "The storage engine of backend store database, like MergeTree/ReplacingMergeTree for Clickhouse.",
//                    disallowEmpty(),
//                    "ReplacingMergeTree"
//            );
}
