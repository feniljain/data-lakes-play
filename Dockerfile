FROM rust:1.81
WORKDIR /app
COPY . .

RUN cargo install --path .

CMD ["tail", "-f", "/dev/null"]
