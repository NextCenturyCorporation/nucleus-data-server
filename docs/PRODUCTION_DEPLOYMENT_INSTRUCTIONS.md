# Neon Server Production Deployment Instructions

## Prerequisites

### Install Dependencies

- [Java 9+](https://www.oracle.com/technetwork/java/javase/downloads/jdk12-downloads-5295953.html)
- [Docker](https://docs.docker.com/v17.12/install/)

## Deploy as Docker Image

### I. Download

1. Download the source code from the GitHub repository: `git clone https://github.com/NextCenturyCorporation/neon-server.git; cd neon-server`

### II. Build

2. (Optional) By default, the Neon Server runs on port `8090`.  If you want to use a different port:

- In `<neon-server>/server/src/main/resources/application.properties`, change the line `server.port=8090` to use your port
- In `Dockerfile`, change the line `EXPOSE 8090` to use your port

3. Build the docker image: `./gradlew clean docker`

4. Run `docker images` to verify that you have created a docker image with the repository `com.ncc.neon/server` and tag `latest`.

### III. Run Locally (Optional)

5. Run the docker container: `docker run -it --network=host --rm com.ncc.neon/server:latest`
