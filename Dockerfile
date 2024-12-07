FROM quay.io/astronomer/astro-runtime:12.5.0

USER root

RUN apt-get update && \
apt-get install -y openjdk-17-jdk && \
apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER astro
