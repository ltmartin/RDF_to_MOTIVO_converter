FROM centos
MAINTAINER Leandro Tabares Martín <ltmartin198@gmail.com>
RUN yum update -y && yum upgrade -y
RUN yum install -y java-11-openjdk.x86_64
RUN mkdir converter
COPY target/RDF_to_MOTIVO_converter-1.0.jar /converter/
COPY target/classes/application.properties /converter/
WORKDIR /converter/
RUN /usr/bin/java -jar RDF_to_MOTIVO_converter-1.0.jar