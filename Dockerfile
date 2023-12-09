FROM --platform=linux/amd64 ubuntu:20.04 as builder

RUN apt update \
    && DEBIAN_FRONTEND=noninteractive apt install -y g++ cmake libelf-dev pkg-config git curl

ENV GOLANG_VERSION 1.20.11
ENV ARCH amd64
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go$GOLANG_VERSION.linux-$ARCH.tar.gz
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
RUN mkdir /go
RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

RUN cd /tmp && git clone https://github.com/magefile/mage && cd mage && go run bootstrap.go

COPY . /beats
RUN cd /beats/x-pack/filebeat && mage build

FROM --platform=linux/amd64 docker.elastic.co/beats/filebeat:8.11.2
COPY --from=builder /beats/x-pack/filebeat/filebeat /usr/share/filebeat/filebeat

