# Apache Flink HTTP Connector

This repository contains the official Apache Flink HTTP connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream and batch processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Quick Start

### SQL Example — HTTP Sink

Use the HTTP Sink connector to write Flink records to an external HTTP endpoint via SQL:

```sql
CREATE TABLE http_sink (
  id     BIGINT,
  name   STRING,
  status STRING
) WITH (
  'connector'     = 'http-sink',
  'url'           = 'https://api.example.com/events',
  'format'        = 'json',
  'insert-method' = 'POST'
);

INSERT INTO http_sink SELECT id, name, status FROM source_table;
```

### SQL Example — HTTP Lookup Source

Use the HTTP Lookup connector to enrich a stream with data from an external HTTP API:

```sql
-- Define the HTTP lookup table
CREATE TABLE http_lookup (
  id      STRING,
  payload STRING
) WITH (
  'connector'     = 'rest-lookup',
  'url'           = 'https://api.example.com/data',
  'format'        = 'json',
  'lookup-method' = 'GET'
);

-- Enrich a stream using a lookup join
SELECT s.event_id, h.payload
FROM stream_table AS s
JOIN http_lookup FOR SYSTEM_TIME AS OF s.proc_time AS h
  ON s.event_id = h.id;
```

### DataStream API Example — HTTP Sink

Use the HTTP Sink connector in the Flink DataStream API:

```java
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

HttpSink<String> httpSink = HttpSink.<String>builder()
    .setEndpointUrl("https://api.example.com/events")
    .setElementConverter(
        (element, context) ->
            new HttpSinkRequestEntry("POST", element.getBytes(StandardCharsets.UTF_8)))
    .build();

env.fromElements("event1", "event2", "event3")
    .sinkTo(httpSink);

env.execute("HTTP Sink Example");
```

### Common Configuration Options

| Option                                      | Required | Default | Description                                                            |
|---------------------------------------------|----------|---------|------------------------------------------------------------------------|
| `connector`                                 | required | —       | Use `http-sink` for sink or `rest-lookup` for lookup source.           |
| `url`                                       | required | —       | The HTTP endpoint URL, e.g. `https://api.example.com/data`.            |
| `format`                                    | required | —       | Serialization format, e.g. `json`.                                     |
| `insert-method`                             | optional | `POST`  | HTTP method for sink: `POST` or `PUT`.                                 |
| `http.sink.request.timeout`                 | optional | `30`    | Request timeout in seconds.                                            |
| `http.sink.writer.request.mode`             | optional | `batch` | Request submission mode: `single` or `batch`.                          |
| `http.sink.request.batch.size`              | optional | `500`   | Number of records per batch request (batch mode only).                 |
| `http.source.lookup.request.timeout`        | optional | —       | Lookup request timeout as a Duration, e.g. `30s`.                     |
| `http.logging.level`                        | optional | `MIN`   | HTTP content logging level: `MIN`, `REQ_RESP`, or `MAX`.               |
| `http.security.cert.server.allowSelfSigned` | optional | `false` | Accept self-signed/untrusted TLS certificates.                         |

For a full list of configuration options and advanced features (TLS, mTLS, OIDC authentication,
retry strategies, proxy support, etc.), refer to the
[official documentation](https://nightlies.apache.org/flink/flink-connector-http-docs/).

## Building the Apache Flink HTTP Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 11

```
git clone https://github.com/apache/flink-connector-http.git
cd flink-connector-http
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective module.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

## Support

Don't hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
