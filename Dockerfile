FROM python:3.11-slim-buster

ENV TZ   Asia/Tokyo
ENV LANG ja_JP.UTF-8

# Copy local scripts into container.
COPY ./quollio_core /root/app/quollio_core
COPY ./Makefile /root/app/
WORKDIR /root/app

RUN set -x \
  && echo "Asia/Tokyo" > /etc/timezone \
  && apt-get update \
  && apt-get -y install \
  git \
  make \
  && cp /usr/share/zoneinfo/Asia/Tokyo /etc/localtime

# Install libraries
ARG LIB_VERSION
RUN set -x \
  && pip install quollio-core==$LIB_VERSION

# Install dbt module
RUN git clone https://github.com/dbt-labs/dbt-utils.git -b 1.1.1 /root/deps/dbt-utils

ENTRYPOINT [ "python", "-m" ]
