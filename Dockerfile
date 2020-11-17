FROM openjdk:15-oracle as build
WORKDIR /workspace/app

COPY rabbit-core rabbit-core
COPY whiterabbit whiterabbit
COPY rabbitinahat rabbitinahat
COPY whiteRabbitService whiteRabbitService
COPY lib lib

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

RUN ./mvnw install

FROM openjdk:15-oracle
VOLUME /tmp

ARG JAR_FILE=/workspace/app/whiteRabbitService/target/*.jar
COPY --from=build ${JAR_FILE} app.jar
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /app.jar ${0} ${@}"]
