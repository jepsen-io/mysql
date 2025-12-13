# This is the configuration image, which AFAICT uses a Docker image just to
# send a single file--the docker-compose.yaml file--to Antithesis.

# But ~before~ we do that, we build a little file corruption program
#FROM alpine:3.22 AS build-source

#RUN apk add build-base gcompat
#COPY corrupt-file.c /corrupt-file.c
#RUN gcc -o corrupt-file corrupt-file.c -lm

# Now our actual config image
FROM scratch

#COPY --from=build-source /corrupt-file /bin/corrupt-file
COPY docker-compose.yaml /docker-compose.yaml

WORKDIR /
