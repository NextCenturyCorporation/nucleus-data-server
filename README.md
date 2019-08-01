# Neon Server

The Neon Server is a REST server that is used with the [Neon Middleware](https://github.com/NextCenturyCorporation/neon-framework) to provide datastore adapters, run datastore queries, transform query results, and perform data processing.  The [Neon Dashboard](https://github.com/NextCenturyCorporation/neon-dash-internal) is a UI that interacts with the Neon Server.

## Prerequisites

* Java 9+
* ES 6.4+

## Modules

* The **server** module contains the core Neon Server code.
* The **esadapter** module contains the Elasticsearch REST datastore adapter.

Change the modules used in your build with the [gradle.properties](./gradle.properties) and [settings.gradle](./settings.gradle) files.

## Properties

Update the properties of your Neon Server in the [server/src/main/resources/application.properties](./server/src/main/resources/application.properties) file.

## Build and Test the Neon Server

To build and test the Neon Server:  `./gradlew build`

## Run the Neon Server

To run the Neon Server:  `./runLocal.sh`

To pass arguments into `bootRun` from the command line, use `--args='<arguments>'`.  For example, to run the Neon Server on a specific port:  `./runLocal.sh --args='--server.port=1234'`

## Build and Run Docker

To build the docker image: `./gradlew clean docker`

To run docker image: `docker run -it --network=host --rm com.ncc.neon/server:latest`

## VSCode Settings

Recommended:

```
{
    "java.home": "<file_path_to_java_home>",
    "java.configuration.updateBuildConfiguration": "automatic"
}
```

## Spring Documentation

* [Spring Boot Reference Guide](https://docs.spring.io/spring-boot/docs/current/reference/html/)
* [Spring Boot Java Docs](https://docs.spring.io/spring-boot/docs/current/api/)
* [Building a Reactive RESTful Web Service](https://spring.io/guides/gs/reactive-rest-service/)
* [Web on Reactive Stack](https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html)

## Apache 2 Open Source License

Neon and  are made available by [Next Century](http://www.nextcentury.com) under the [Apache 2 Open Source License](http://www.apache.org/licenses/LICENSE-2.0.txt). You may freely download, use, and modify, in whole or in part, the source code or release packages. Any restrictions or attribution requirements are spelled out in the license file. Neon and  attribution information can be found in the [LICENSE](./LICENSE) and [NOTICE](./NOTICE.md) files. For more information about the Apache license, please visit the [The Apache Software Foundation’s License FAQ](http://www.apache.org/foundation/license-faq.html).

## Contact Us

Email: neon-support@nextcentury.com

Website: http://neonframework.org

Copyright 2019 Next Century Corporation
