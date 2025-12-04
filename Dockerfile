# Stage 1: Build a minimal custom JRE
FROM amazoncorretto:21-alpine-jdk AS builder

# Required for --strip-debug to work on Alpine
RUN apk add --no-cache binutils

WORKDIR /app
COPY target/*.jar app.jar

# Build small JRE image
RUN $JAVA_HOME/bin/jlink \
    --verbose \
    --add-modules java.base,java.management,java.naming,java.net.http,java.security.jgss,java.security.sasl,java.sql,jdk.httpserver,jdk.unsupported \
    --strip-debug \
    --no-man-pages \
    --no-header-files \
    --compress=zip-6 \
    --output /custom-jre

# Stage 2: Runtime image
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata curl \
    && addgroup -g 1001 -S appgroup \
    && adduser -u 1001 -S appuser -G appgroup

COPY --from=builder /custom-jre /opt/java
WORKDIR /app
COPY --from=builder --chown=appuser:appgroup /app/app.jar app.jar

ENV JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:$PATH"

USER appuser:appgroup
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "/app/app.jar"]