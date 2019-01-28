# Neon Overview

Neon is a software platform designed to help you, as a developer, to integrate your disparate visualization widgets with your data stores. 

# Neon Server
Provides the Data Access API makes it easy for widgets to query NoSQL databases directly from JavaScript or RESTful endpoints, while still letting the server do the heavy lifting. Aslo providing, the Interaction API provides capabilities for inter-widget communication, which easily links your widgets together. Neon Server does not provide any user interface components. Instead, Neon Server shines under-the-hood by removing the pressure from developers to figure out how to make different components work together and allowing them to focus more on the fun stuff, like creating valuable data exploration applications and workflows.

## Prerequisites
* Java 9

## Running Application with Gradle

```
./gradlew bootRun`
```

### Passing arguments to your application

Arguments can be passed into bootRun from the command line using `--args='<arguments>'`. For example, to run your application with a different port:

```
$ ./gradlew bootRun --args='--server.port=8090'
```
## License

This project is licensed under the  Apache License Version 2.0 - see the [LICENSE](LICENSE) file for details

## Additional Information

Email: neon-support@nextcentury.com

Website: [http://neonframework.org]

Copyright 2018 Next Century Corporation