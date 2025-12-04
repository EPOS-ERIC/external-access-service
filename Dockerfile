# Stage 1: Build a minimal custom JRE
FROM amazoncorretto:21-alpine-jdk AS builder

WORKDIR /app
COPY target/*.jar app.jar

# Extract layers for better caching (Spring Boot)
RUN java -Djarmode=layertools -jar app.jar extract || true

# Create a minimal custom JRE (adjust modules as needed)
RUN jlink \
    --add-modules java.base,java.logging,java.sql,java.naming,java.desktop,java.management,java.security.jgss,java.instrument \
    --strip-debug \
    --compress=2 \
    --no-header-files \
    --no-man-pages \
    --output /custom-jre

# Stage 2: Runtime image
FROM alpine:3.20

# Add labels for metadata
LABEL maintainer="your-team@example.com" \
      version="1.0" \
      description="Application description"

# Install only essential packages and create non-root user
RUN apk --no-cache add ca-certificates tzdata curl \
    && addgroup -g 1001 -S appgroup \
    && adduser -u 1001 -S appuser -G appgroup

# Copy custom JRE from builder
COPY --from=builder /custom-jre /opt/java

# Set up application directory
WORKDIR /app

# Copy application (use layered approach if Spring Boot)
COPY --from=builder --chown=appuser:appgroup /app/app.jar app.jar

# Environment variables
ENV JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:$PATH" \
    JAVA_OPTS=""

# Switch to non-root user
USER appuser:appgroup

# Expose port (adjust as needed)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

# Use exec form and allow JAVA_OPTS override
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]