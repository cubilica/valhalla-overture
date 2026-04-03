cmake_minimum_required(VERSION 3.14)
project(valhalla_overture VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# Valhalla is installed as a static library in /usr/local
# Find its transitive dependencies via pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
pkg_check_modules(SPATIALITE REQUIRED spatialite)
pkg_check_modules(SQLITE3 REQUIRED sqlite3)
pkg_check_modules(LUA REQUIRED luajit)
pkg_check_modules(GEOS REQUIRED geos)
pkg_check_modules(ZLIB REQUIRED zlib)
pkg_check_modules(LZ4 REQUIRED liblz4)
pkg_check_modules(CURL REQUIRED libcurl)
pkg_check_modules(SSL REQUIRED openssl)

add_executable(overture_build_tiles
  src/main.cc
  src/overture_parser.cc
)

target_include_directories(overture_build_tiles PRIVATE
  src
  /usr/local/include
  /usr/local/include/valhalla/third_party
  ${PROTOBUF_INCLUDE_DIRS}
)

target_link_libraries(overture_build_tiles
  /usr/local/lib/libvalhalla.a
  /usr/local/lib/libprime_server.a
  Arrow::arrow_shared
  Parquet::parquet_shared
  ${PROTOBUF_LIBRARIES}
  ${SPATIALITE_LIBRARIES}
  ${SQLITE3_LIBRARIES}
  ${LUA_LIBRARIES}
  ${GEOS_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${LZ4_LIBRARIES}
  ${CURL_LIBRARIES}
  ${SSL_LIBRARIES}
  zmq
  czmq
  expat
  pthread
  dl
)
