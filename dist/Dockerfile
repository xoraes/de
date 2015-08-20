# docker build -t pixelle/pxldesrv

FROM java:8

COPY . /usr/src/myapp
WORKDIR /usr/src/myapp

RUN ./gradlew jar de distZip
RUN unzip -d / build/distributions/de.zip
RUN rm -rf /usr/src/myapp

COPY dist/de.conf /etc/de.conf

VOLUME ["/data"]

ENTRYPOINT ["/de/bin/de"]
