set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

execute_process(
  COMMAND "${Python3_EXECUTABLE}" -m pybind11 --cmakedir
  OUTPUT_VARIABLE pybind11_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(pybind11_DIR "${pybind11_DIR}" CACHE PATH "pybind11 CMake directory")

find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(searchcore
  searchcore.cpp
  lsmStore.cpp)
target_compile_features(searchcore PRIVATE cxx_std_20)
target_include_directories(searchcore PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(searchcore PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}
)

find_package(RocksDB REQUIRED)
target_link_libraries(searchcore PRIVATE RocksDB::rocksdb)

add_executable(test_token test.cpp)
target_compile_definitions(test_token PRIVATE PROJECTROOT_DIR="${PROJECTROOT_DIR}")
target_include_directories(test_token PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})