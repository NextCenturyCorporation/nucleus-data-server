# NUCLEUS Data Server

The NUCLEUS Data Server is a REST server that is used with [NUCLEUS](https://github.com/NextCenturyCorporation/nucleus) to provide datastore adapters, run datastore queries, transform query results, and perform data processing.  The [Neon Dashboard](https://github.com/NextCenturyCorporation/neon-dash-internal) is a UI that interacts with the NUCLEUS Data Server.

## Table of Content

* [Initial Setup Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#initial-setup-instructions)
* [Datastore Authentication](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-authentication)
* [Local Development Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#local-development-instructions)
* [Production Deployment Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#production-deployment-instructions)
* [Datastore Support](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-support)
* [Datastore Configuration](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-configuration)
* [Technical Stack](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#technical-stack)
* [Architecture Documentation](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#architecture-documentation)
* [License](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#apache-2-open-source-license)
* [Contact Us](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#contact-us)

## Initial Setup Instructions

### Install Dependencies

- [Java 9+](https://www.oracle.com/technetwork/java/javase/downloads/jdk12-downloads-5295953.html) (Tested on OpenJDK versions 11 and 12)
- [Docker](https://docs.docker.com/v17.12/install/)
- A [supported datastore](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-support)

### Download Source Code

`git clone https://github.com/NextCenturyCorporation/neon-server.git; cd neon-server`

### Load Data

If you were given a sample data bundle by the Neon / NUCLEUS Development Team, please download it and follow the specific instructions in its README file.

If you want to use your own data, please see the [Datastore Configuration](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-configuration) for more information.

### Customize Build (Optional)

Update the runtime properties of your NUCLEUS Data Server installation by editing the [server/src/main/resources/application.properties](./server/src/main/resources/application.properties) file.  See the [Spring Boot Configuration Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html) for details.  Please note that the `server.servlet.context-path` application property should always end with `/services` due to current assumptions made in NUCLEUS.

## Datastore Authentication

### Basic Auth

In the [server/src/main/resources/application.properties](./server/src/main/resources/application.properties) file, add the following property, replacing `my_datastore_type` with your datastore type (`elasticsearch`, `mysql`, `postgresql`):

```
my_datastore_type.auth={'hostname':'username:password'}
```

Replace the `hostname`, `username`, and `password` as needed.  The `hostname` can be an IP address or a CNAME and can optionally have a port.  If you need multiple authentication entries, separate them with commas:

```
my_datastore_type.auth={'hostname1':'username1:password1','hostname2':'username2:password2'}
```

## Local Development Instructions

### Build and Run Tests

`./gradlew build`

### Run Locally

`./runLocal.sh`

This will run `bootRun` from the [Spring Boot Gradle plugin](https://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-running-your-application.html#using-boot-running-with-the-gradle-plugin).  To pass custom arguments into `bootRun` from the command line, use `--args='<arguments>'.  For example, to run on a specific port: `./runLocal.sh --args='--server.port=1234'`

## Production Deployment Instructions

The NUCLEUS Data Server is deployed as an docker container independent from other applications (like the Neon Dashboard).

### Deploy as Docker Container

#### 1. Perform All Initial Setup

Follow the [Initial Setup Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#initial-setup-instructions) above.

#### 2. (Optional) Update the NUCLEUS Data Server's Port

By default, the NUCLEUS Data Server runs on port `8090`.  If you want to use a different port:

- In `<neon-server>/server/src/main/resources/application.properties`, change the line `server.port=8090` to use your port
- In `Dockerfile`, change the line `EXPOSE 8090` to use your port

#### 3. Build the Docker Image

`./gradlew clean docker`

#### 4. Verify the Docker Image

Run `docker images` to verify that you have created a docker image with the repository `com.ncc.neon/server` and tag `latest`.

#### 5. (Optional) Run the Docker Container Locally

`docker run -it --network=host --rm com.ncc.neon/server:latest`

## Datastore Support

The NUCLEUS Data Server supports the following datastores:

- [Elasticsearch 6.7 - 6.8](https://www.elastic.co/downloads/past-releases/elasticsearch-6-8-1)
- [Elasticsearch 7](https://www.elastic.co/downloads/elasticsearch)
- [MySQL](https://www.mysql.com/downloads/)
- [PostgreSQL](https://www.postgresql.org/download/)

Elasticsearch support provided by the official [Java High Level REST Client](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high.html).  SQL support provided by [R2DBC drivers](https://r2dbc.io).

*Want us to support another datastore?  [Please let us know!](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#contact-us)*

## Datastore Configuration

### Elasticsearch 6 and 7

We recommend installing [elasticdump](https://www.npmjs.com/package/elasticdump) to load bulk data: `npm install -g elasticdump`

If you have previously installed elasticdump, we recommend that you rerun the command to update it to its latest version.

#### Elasticsearch Data Format

If your data is spread across multiple indexes, we recommend that you [denormalize](https://www.elastic.co/guide/en/elasticsearch/guide/current/denormalization.html) any fields that you want to use as filters.

#### Elasticsearch Mapping Files

It's usually very important to load a mapping file associated with your data index into Elasticsearch BEFORE loading any data into that index.

If you HAVE loaded data before loading your mapping file, you'll either need to [reindex](https://www.elastic.co/guide/en/elasticsearch/reference/6.4/docs-reindex.html) your data index or delete the index and start over again.

More information about mapping files can be found on the [Elasticsearch website](https://www.elastic.co/guide/en/elasticsearch/reference/6.4/mapping.html).

#### Elasticsearch Date Fields

Date fields should have the `format` `"yyyy-MM-dd||dateOptionalTime||E MMM d HH:mm:ss zzz yyyy"`.  For example:

```json
"timestamp": {
    "type": "date",
    "format": "yyyy-MM-dd||dateOptionalTime||E MMM d HH:mm:ss zzz yyyy"
}
```

Note that you may need to add an additional date format to the end of the `format` string (separated by two pipe characters `||`).  For example, if the dates in your data look like `12/25/2018 01:23:45`, you would use the following `format` string:

```json
"date_field": {
    "type": "date",
    "format": "yyyy-MM-dd||dateOptionalTime||E MMM d HH:mm:ss zzz yyyy||MM/dd/yyyy HH:mm:ss"
}
```

For more information on date format mappings, please see the [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html).

#### Elasticsearch Keyword Fields

We recommend that any string field not containing document text (including news articles, social media posts, or any multi-sentence text field) should have the `type` `keyword`.  For example, fields of names, links, categories, and alphanumeric IDs should all have the `type` `keyword`.

```json
"name": {
    "type": "keyword"
}
```

#### Elasticsearch Text Fields

Text fields should have the `fielddata` property set to `true`.  For example:

```json
"social_media_post": {
    "type": "text",
    "fielddata": true
}
```

#### Elasticsearch Data Ingest Tips

Data file format:  define individual JSON objects on separate lines in the file.  Example:

```json
{ "_index": "index_name", "_type": "index_mapping_type", "_source": { "whatever_field": "whatever values" }}
{ "_index": "index_name", "_type": "index_mapping_type", "_source": { "whatever_field": "more values" }}
```

**CURL** Mapping file format:  start with the "properties".  Example:

```json
{
  "properties": {
    "whatever_field": {
      "type": "whatever_type"
    }
  }
}
```

**CURL** [ES6] Create an index:

```
curl -XPUT hostname:port/index_name
curl -XPUT hostname:port/index_name/_mapping/index_mapping_type -H "Content-Type: application/json" -d @mapping_file.json
```

**CURL** [ES7] Create an index:

```
curl -XPUT hostname:port/index_name
curl -XPUT hostname:port/index_name/_mapping -H "Content-Type: application/json" -d @mapping_file.json
```

**CURL** Delete an index:

```
curl -XDELETE hostname:port/index_name
```

**ELASTICDUMP** Mapping file format:  start with the index name.  Example:

```json
{
  "index_name": {
    "mappings": {
      "index_mapping_type": {
        "properties": {
          "whatever_field": {
            "type": "whatever_type"
          }
        }
      }
    }
  }
}
```

**ELASTICDUMP** Create an index:

```
elasticdump --type=mapping --input=mapping_file.json --output=hostname:port/index_name
```

**ELASTICDUMP** Ingest data into an index:

```
elasticdump --type=data --limit=10000 --input=data_file.json --output=hostname:port/index_name
```

## Technical Stack

The NUCLEUS Data Server is a [Spring Boot](https://spring.io/projects/spring-boot) WebFlux Java application built using the Gradle plugins.

### Modules

The application is built using multiple custom modules:

* The **server** module contains the core Data Server code.
* The **sqladapter** module contains the SQL JDBC datastore adapter that currently supports MySQL and PostgreSQL.
* The **esadapter** module contains the Elasticsearch REST datastore adapter.
* The **common** module contains the common adapter and model classes.

Change the modules included in your build (to add or remove adapter dependencies) by editing the [gradle.properties](./gradle.properties) and [settings.gradle](./settings.gradle) files.

### Spring Documentation

* [Spring Boot Reference Guide](https://docs.spring.io/spring-boot/docs/current/reference/html/)
* [Spring Boot Java Docs](https://docs.spring.io/spring-boot/docs/current/api/)
* [Building a Reactive RESTful Web Service](https://spring.io/guides/gs/reactive-rest-service/)
* [Web on Reactive Stack](https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html)

## Architecture Documentation

### Field Types

NUCLEUS returns the following field types from its `queryservice/fields/types` endpoint:

- `boolean`
- `date`
- `decimal`
- `geo`
- `id`
- `integer`
- `keyword`
- `object`
- `text`

## Apache 2 Open Source License

NUCLEUS is made available by [Next Century](http://www.nextcentury.com) under the [Apache 2 Open Source License](http://www.apache.org/licenses/LICENSE-2.0.txt). You may freely download, use, and modify, in whole or in part, the source code or release packages. Any restrictions or attribution requirements are spelled out in the license file. NUCLEUS attribution information can be found in the [LICENSE](./LICENSE) file. For more information about the Apache license, please visit the [The Apache Software Foundation’s License FAQ](http://www.apache.org/foundation/license-faq.html).

## Contact Us

Email: [neon-support@nextcentury.com](mailto:neon-support@nextcentury.com)

Website: http://neonframework.org

Copyright 2019 Next Century Corporation
