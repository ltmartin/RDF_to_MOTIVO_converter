FROM debian:stable
MAINTAINER Leandro Tabares Martín <ltmartin198@gmail.com>
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y openjdk-17-jre
RUN mkdir converter
COPY target/RDF_to_MOTIVO_converter-1.0.jar /converter/
COPY src/main/resources/application.properties /converter/
WORKDIR /converter
RUN mkdir output
VOLUME ["/converter/output"]
CMD ["java", "-jar", "RDF_to_MOTIVO_converter-1.0.jar"]
