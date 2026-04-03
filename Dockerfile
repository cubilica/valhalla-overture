FROM ghcr.io/valhalla/valhalla:latest

# Install build tools, valhalla transitive deps, and Arrow/Parquet
RUN apt-get update && apt-get install -y \
  cmake make g++ wget lsb-release gnupg ca-certificates pkg-config \
  libzmq3-dev libczmq-dev libprotobuf-dev \
  libspatialite-dev libsqlite3-dev libluajit-5.1-dev \
  libgeos-dev libssl-dev libcurl4-openssl-dev liblz4-dev zlib1g-dev \
  libboost-all-dev && \
  wget -q https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr '[:upper:]' '[:lower:]')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt-get update && \
  apt-get install -y libarrow-dev libparquet-dev && \
  rm -rf /var/lib/apt/lists/* apache-arrow-apt-source-*.deb

WORKDIR /src
COPY CMakeLists.txt .
COPY src/ src/
RUN cmake -B build . && cmake --build build && \
  cp build/overture_build_tiles /usr/local/bin/ && \
  rm -rf build /src

ENTRYPOINT ["overture_build_tiles"]
