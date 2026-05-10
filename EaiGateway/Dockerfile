# ==========================================
# Stage 1: Build the Gateway jar
# ==========================================
FROM gradle:jdk21 AS builder
WORKDIR /app

# Copy the source code from the GitHub subfolder
COPY . .

# Compile the application, skipping tests for speed
RUN gradle clean build -x test

# ==========================================
# Stage 2: Run the Gateway
# ==========================================
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Copy ONLY the built jar file from the builder stage
COPY --from=builder /app/build/libs/*.jar app.jar

# Expose the Gateway ports
EXPOSE 8080
EXPOSE 32080

# Start the application
ENTRYPOINT ["java", "-jar", "app.jar"]