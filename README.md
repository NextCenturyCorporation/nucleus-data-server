# Neon Server

The Neon Server is a REST server that is used with the [Neon Middleware](https://github.com/NextCenturyCorporation/neon-framework) to provide datastore adapters, run datastore queries, transform query results, and perform data processing.  The [Neon Dashboard](https://github.com/NextCenturyCorporation/neon-dash-internal) is a UI that interacts with the Neon Server.

## Technical Stack

The Neon Server is a Spring Boot WebFlux Java application built using Gradle plugins.

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

## Local Development

Please see the [Local Development Instructions](./docs/LOCAL_DEVELOPMENT_INSTRUCTIONS.md)

## Production Deployment

Please see the [Production Deployment Instructions](./docs/PRODUCTION_DEPLOYMENT_INSTRUCTIONS.md)

## Apache 2 Open Source License

Neon and  are made available by [Next Century](http://www.nextcentury.com) under the [Apache 2 Open Source License](http://www.apache.org/licenses/LICENSE-2.0.txt). You may freely download, use, and modify, in whole or in part, the source code or release packages. Any restrictions or attribution requirements are spelled out in the license file. Neon and  attribution information can be found in the [LICENSE](./LICENSE) and [NOTICE](./NOTICE.md) files. For more information about the Apache license, please visit the [The Apache Software Foundation’s License FAQ](http://www.apache.org/foundation/license-faq.html).

## Contact Us

Email: neon-support@nextcentury.com

Website: http://neonframework.org

Copyright 2019 Next Century Corporation
