# Stage 1: Build a minimal custom JRE
FROM amazoncorretto:21-alpine-jdk AS builder

WORKDIR /app
COPY target/*.jar app.jar

# First, find required modules for your app
RUN jar --file=app.jar --describe-module || true

# Create a minimal custom JRE (JDK 21+ uses different compress syntax)
RUN jlink \
    --add-modules java.base,java.logging,java.sql,java.naming,java.management,java.security.jgss,java.instrument,java.net.http \
    --strip-debug \
    --compress=zip-6 \
    --no-header-files \
    --no-man-pages \
    --output /custom-jre

# Stage 2: Runtime image
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata curl \
    && addgroup -g 1001 -S appgroup \
    && adduser -u 1001 -S appuser -G appgroup

COPY --from=builder /custom-jre /opt/java
WORKDIR /app
COPY --from=builder --chown=appuser:appgroup /app/app.jar app.jar

ENV JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:$PATH" \
    JAVA_OPTS=""

USER appuser:appgroup
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]