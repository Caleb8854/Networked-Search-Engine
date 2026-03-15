set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

execute_process(
  COMMAND "${Python3_EXECUTABLE}" -m pybind11 --cmakedir
  OUTPUT_VARIABLE pybind11_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(pybind11_DIR "${pybind11_DIR}" CACHE PATH "pybind11 CMake directory")

find_package(pybind11 CONFIG REQUIRED)

find_library(ROCKSDB_LIB rocksdb REQUIRED)

pybind11_add_module(searchcore
  searchcore.cpp
  lsmStore.cpp)
target_compile_features(searchcore PRIVATE cxx_std_20)
target_include_directories(searchcore PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(searchcore PRIVATE ${ROCKSDB_LIB})

set_target_properties(searchcore PROPERTIES
	LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)

