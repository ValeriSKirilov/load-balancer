FROM ubuntu:24.04

RUN apt-get update && \ 
    apt-get install -y build-essential cmake && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY CMakeLists.txt backend.conf backend.docker.conf ./
COPY include ./include
COPY src ./src
COPY dashboard ./dashboard

RUN cmake -B build && cmake --build build

EXPOSE 8080

CMD ["./build/load_balancer", "ROUND_ROBIN", "4", "backend.docker.conf"]