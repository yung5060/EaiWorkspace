# ==========================================
# Stage 1: Build the Config Server jar
# ==========================================
FROM gradle:jdk21 AS builder
WORKDIR /app

# Copy the source code from the GitHub subfolder
COPY . .

# Compile the application, skipping tests for speed
RUN gradle clean build -x test

# ==========================================
# Stage 2: Run the Config Server
# ==========================================
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Copy ONLY the built jar file from the builder stage
COPY --from=builder /app/build/libs/*.jar app.jar

# Expose the Config Server port
EXPOSE 8888

# Start the application
ENTRYPOINT ["java", "-jar", "app.jar"]