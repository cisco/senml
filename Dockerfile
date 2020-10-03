FROM ubuntu:latest as builder
#FROM ubuntu:latest 
LABEL description="Run SenML HTTP to influx server"

#ENV TZ=America/Vancouver
#RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update
# RUN apt-get install -y tzdata build-essential cmake 
RUN apt-get install -y git make gcc
RUN apt-get install -y curl 

WORKDIR /tmp/go
RUN curl -O https://dl.google.com/go/go1.15.2.linux-amd64.tar.gz
RUN tar xvf go*linux-amd64.tar.gz
RUN chown -R root:root ./go
RUN mv go /usr/local

WORKDIR /usr/src/senml
COPY . . 

ENV GOPATH /root/work
ENV PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/go/bin:/root/work/bin
ENV CGO_ENABLED 0
ENV GOOS linux

RUN go get github.com/ugorji/go/codec
RUN go build -a -installsuffix cgo -ldflags '-extldflags "-static"'  . 
RUN go build -a -installsuffix cgo -ldflags '-extldflags "-static"'  ./cmd/senmlCat/.
RUN go build -a -installsuffix cgo -ldflags '-extldflags "-static"'  ./cmd/senmlServer/.
RUN cp ./senmlCat /usr/local/bin 
RUN cp ./senmlServer /usr/local/bin 


FROM alpine
#RUN apk add --no-cache bash

COPY --from=builder /usr/local/bin/senmlServer /usr/local/bin/senmlServer
COPY --from=builder /usr/local/bin/senmlCat /usr/local/bin/senmlCat

RUN adduser -S -D -H -h /app senml
USER senml

CMD senmlCat
