# Use an OpenJDK 17 base image
FROM eclipse-temurin:17-jdk

# Create app directory
WORKDIR /app

# Copy the built jar into the image
COPY target/kafka-consumer-app-1.0.0-jar-with-dependencies.jar /app/app.jar

# Run the app
CMD ["java", "-jar", "/app/app.jar"]
