FROM amazoncorretto:21-alpine-jdk

RUN apk update && apk add --no-cache ca-certificates && apk upgrade --no-cache ca-certificates && update-ca-certificates

ADD target/*.jar app.jar

RUN apk --no-cache add curl

ENTRYPOINT ["java" , "-Djava.security.egd=file:/dev/./urandom","-jar","/app.jar"]
