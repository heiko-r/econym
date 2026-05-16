# syntax=docker/dockerfile:1
FROM scratch

ARG TARGETARCH

COPY artifacts/${TARGETARCH}/econym /econym

EXPOSE 3000

ENTRYPOINT ["/econym"]
