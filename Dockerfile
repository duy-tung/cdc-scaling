FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /nomios ./cmd/nomios

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /nomios /nomios
COPY configs/nomios.yaml /etc/nomios/nomios.yaml
EXPOSE 8080
ENTRYPOINT ["/nomios"]
CMD ["-config", "/etc/nomios/nomios.yaml"]
