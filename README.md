# Neon Server

The Neon Server is a REST server that is used with the [Neon Middleware](https://github.com/NextCenturyCorporation/neon-framework) to provide datastore adapters, run datastore queries, transform query results, and perform data processing.  The [Neon Dashboard](https://github.com/NextCenturyCorporation/neon-dash-internal) is a UI that interacts with the Neon Server.

## Table of Content

* [Initial Setup Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#initial-setup-instructions)
* [Local Development Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#local-development-instructions)
* [Production Deployment Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#production-deployment-instructions)
* [Datastore Support](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-support)
* [Datastore Configuration](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-configuration)
* [Technical Stack](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#technical-stack)
* [License](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#apache-2-open-source-license)
* [Contact Us](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#contact-us)

## Initial Setup Instructions

### Download Source Code

`git clone https://github.com/NextCenturyCorporation/neon-server.git; cd neon-server`

### Install Dependencies

- [Java 9+](https://www.oracle.com/technetwork/java/javase/downloads/jdk12-downloads-5295953.html)
- [Docker](https://docs.docker.com/v17.12/install/)
- A [supported datastore](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-support)

### Load Data

If you were given a sample data bundle by the Neon Development Team, please download it and follow its specific README instructions.

If you want to use your own data, please see the [datastore configuration](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#datastore-configuration) for more information.

### Customize Build (Optional)

Update the runtime properties of your Neon Server installation in the [server/src/main/resources/application.properties](./server/src/main/resources/application.properties) file.  See the [Spring Boot Configuration Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html) for details.

## Local Development Instructions

### Build and Run Tests

To build and test the Neon Server: `./gradlew build`

### Run Locally

To run the Neon Server: `./runLocal.sh`

This will run `bootRun` from the [Spring Boot Gradle plugin](https://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-running-your-application.html#using-boot-running-with-the-gradle-plugin).  To pass custom arguments into `bootRun` from the command line, use `--args='<arguments>'.  For example, to run on a specific port: `./runLocal.sh --args='--server.port=1234'`

## Production Deployment Instructions

1. Follow the [Initial Setup Instructions](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#initial-setup-instructions) above.

### Build

2. (Optional) By default, the Neon Server runs on port `8090`.  If you want to use a different port:

- In `<neon-server>/server/src/main/resources/application.properties`, change the line `server.port=8090` to use your port
- In `Dockerfile`, change the line `EXPOSE 8090` to use your port

3. Build the docker image: `./gradlew clean docker`

4. Run `docker images` to verify that you have created a docker image with the repository `com.ncc.neon/server` and tag `latest`.

### Run Locally (Optional)

5. Run the docker container: `docker run -it --network=host --rm com.ncc.neon/server:latest`

## Datastore Support

The Neon Server currently supports the following datastores:

- [Elasticsearch 6.4+](https://www.elastic.co/downloads/past-releases/elasticsearch-6-8-1)

*Want us to support other datastores?  [Please let us know!](https://github.com/NextCenturyCorporation/neon-server/blob/master/README.md#contact-us)*

## Datastore Configuration

### Elasticsearch 6

We recommend installing [elasticdump](https://www.npmjs.com/package/elasticdump) to load bulk data: `npm install -g elasticdump`

#### Elasticsearch 6 Mapping Files

It's usually very important to load a mapping file associated with your data index into Elasticsearch BEFORE loading any data into that index.

If you HAVE loaded data before loading your mapping file, you'll either need to [reindex](https://www.elastic.co/guide/en/elasticsearch/reference/6.4/docs-reindex.html) your data index or delete the index and start over again.

More information about mapping files can be found on the [Elasticsearch website](https://www.elastic.co/guide/en/elasticsearch/reference/6.4/mapping.html).

#### Elasticsearch 6 Date Fields

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

#### Elasticsearch 6 Keyword Fields

We recommend that any string field not containing document text (including news articles, social media posts, or any multi-sentence text field) should have the `type` `keyword`.  For example, fields of names, links, categories, and alphanumeric IDs should all have the `type` `keyword`.

```json
"name": {
    "type": "keyword"
}
```

#### Elasticsearch 6 Text Fields

Text fields should have the `fielddata` property set to `true`.  For example:

```json
"social_media_post": {
    "type": "text",
    "fielddata": true
}
```

## Technical Stack

The Neon Server is a [Spring Boot](https://spring.io/projects/spring-boot) WebFlux Java application built using Gradle plugins.

### Modules

The application is built using multiple custom modules:

* The **server** module contains the core Neon Server code.
* The **esadapter** module contains the Elasticsearch REST datastore adapter.
* The **common** module contains the common adapter and model classes.

Change the modules included in your build (to add or remove adapter dependencies) by editing the [gradle.properties](./gradle.properties) and [settings.gradle](./settings.gradle) files.

### Spring Documentation

* [Spring Boot Reference Guide](https://docs.spring.io/spring-boot/docs/current/reference/html/)
* [Spring Boot Java Docs](https://docs.spring.io/spring-boot/docs/current/api/)
* [Building a Reactive RESTful Web Service](https://spring.io/guides/gs/reactive-rest-service/)
* [Web on Reactive Stack](https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html)

## Apache 2 Open Source License

Neon and  are made available by [Next Century](http://www.nextcentury.com) under the [Apache 2 Open Source License](http://www.apache.org/licenses/LICENSE-2.0.txt). You may freely download, use, and modify, in whole or in part, the source code or release packages. Any restrictions or attribution requirements are spelled out in the license file. Neon and  attribution information can be found in the [LICENSE](./LICENSE) and [NOTICE](./NOTICE.md) files. For more information about the Apache license, please visit the [The Apache Software Foundation’s License FAQ](http://www.apache.org/foundation/license-faq.html).

## Contact Us

Email: [neon-support@nextcentury.com](mailto:neon-support@nextcentury.com)

Copyright 2019 Next Century Corporation
