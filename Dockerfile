FROM openjdk:10-jdk-slim
VOLUME /tmp
EXPOSE 8090
COPY classes /app 
COPY libs /app/lib
COPY meta /app/META-INF
ENTRYPOINT ["java","-cp","app:app/lib/*","com.ncc.neon.NeonServerApplication"]
