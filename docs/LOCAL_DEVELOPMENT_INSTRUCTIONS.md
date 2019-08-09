# Neon Server Local Development Instructions

## Prerequisites

### Install Dependencies

- [Java 9+](https://www.oracle.com/technetwork/java/javase/downloads/jdk12-downloads-5295953.html)
- a [supported datastore](./DATA_SUPPORT_GUIDE.md)

### Load Data

If you were given a sample data bundle by the Neon Development Team, please download it and follow its specific README instructions.

If you want to use your own data, please see the [Neon Data Support Guide](https://github.com/NextCenturyCorporation/neon-server/blob/master/DATA_SUPPORT_GUIDE.md) for more information.

### Customize Build (Optional)

Update the runtime properties of your Neon Server installation in the [server/src/main/resources/application.properties](./server/src/main/resources/application.properties) file.  See the [Spring Boot Configuration Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html) for details.

## Build and Run Tests

To build and test the Neon Server: `./gradlew build`

## Run Locally

To run the Neon Server: `./runLocal.sh`

This will run `bootRun` from the [Spring Boot Gradle plugin](https://docs.spring.io/spring-boot/docs/current/reference/html/using-boot-running-your-application.html#using-boot-running-with-the-gradle-plugin).  To pass custom arguments into `bootRun` from the command line, use `--args='<arguments>'.  For example, to run on a specific port: `./runLocal.sh --args='--server.port=1234'`

## Docker Deployment

Please see the [Neon Server Production Deployment Instructions](./PRODUCTION_DEPLOYMENT_INSTRUCTIONS.md)
