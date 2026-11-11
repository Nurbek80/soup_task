FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/soup ./cmd/soup

FROM alpine:3.22
RUN adduser -D -u 10001 soup
USER soup
COPY --from=build /out/soup /usr/local/bin/soup
ENTRYPOINT ["soup"]
