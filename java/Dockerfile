# The Java implementation as an image with `dp` as its entrypoint:
#   docker build -f java/Dockerfile -t decoupled-payments-java .     (from the repository root)
#   docker run --rm decoupled-payments-java validator --siteId=2
# The build needs the shared schemas/ and config/default.yaml next to java/, so the context is the repo root.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY schemas/ schemas/
COPY config/default.yaml config/
COPY java/mvnw java/pom.xml java/
COPY java/.mvn/ java/.mvn/
COPY java/src/ java/src/
WORKDIR /src/java
# The cache mount keeps Maven and its downloads between builds; the unit tests run as part of the build.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q package

FROM eclipse-temurin:21-jre
COPY --from=build /src/java/target/dp.jar /opt/dp/dp.jar
RUN printf '#!/bin/sh\nexec java $JAVA_OPTS -jar /opt/dp/dp.jar "$@"\n' > /usr/local/bin/dp \
    && chmod 755 /usr/local/bin/dp \
    && useradd --system --no-create-home --shell /usr/sbin/nologin dp
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
USER dp
ENTRYPOINT ["dp"]
