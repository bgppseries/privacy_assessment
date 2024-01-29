FROM neo4j:3.4.4

LABEL maintainer="Curo Heros"

ENV APOC_VERSION 3.4.0.1
ENV APOC_URI https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/${APOC_VERSION}/apoc-${APOC_VERSION}-all.jar

RUN wget $APOC_URI && mv apoc-${APOC_VERSION}-all.jar plugins/apoc-${APOC_VERSION}-all.jar

EXPOSE 7474 7473 7687

CMD ["neo4j"]