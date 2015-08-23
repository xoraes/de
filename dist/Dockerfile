FROM jeanblanchard/java:jre-8

ADD build/distributions/de.zip /
RUN unzip -d / /de.zip &&  rm /de.zip

RUN apk add --update bash && rm -rf /var/cache/apk/*

COPY dist/de.conf /etc/de.conf

RUN mkdir -p /var/log/elasticsearch

RUN mkdir /data
VOLUME ["/data"]

ENTRYPOINT ["/de/bin/de"]
