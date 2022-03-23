FROM openjdk:17 as build
WORKDIR /workspace/app

COPY rabbit-core rabbit-core
COPY whiterabbit whiterabbit
COPY whiteRabbitService whiteRabbitService
COPY lib lib

COPY mvnw .
COPY .mvn .mvn
COPY pom.xml .

RUN tr -d '\015' <./mvnw >./mvnw.sh && mv ./mvnw.sh ./mvnw && chmod 770 mvnw

RUN ./mvnw package

FROM openjdk:17
VOLUME /tmp

ARG JAR_FILE=/workspace/app/whiteRabbitService/target/*.jar
COPY --from=build ${JAR_FILE} app.jar
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /app.jar ${0} ${@}"]
