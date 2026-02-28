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

pybind11_add_module(searchcore searchcore.cpp)
target_include_directories(searchcore PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(test_token test.cpp)
target_include_directories(test_token PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_compile_definitions(test_token PRIVATE PROJECTROOT_DIR="${PROJECTROOT_DIR}")