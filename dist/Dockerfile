FROM jeanblanchard/java:jre-8

RUN apk add --update bash && rm -rf /var/cache/apk/*

ADD dist/dockerize /
COPY dist/de.conf.tmpl /etc/de.conf.tmpl

RUN mkdir -p /var/log/elasticsearch


RUN mkdir /data

ADD build/distributions/de.zip /
RUN unzip -d / /de.zip &&  rm /de.zip

VOLUME ["/data"]

ENTRYPOINT ["/dockerize", "-template=/etc/de.conf.tmpl:/etc/de.conf", "/de/bin/de"]
