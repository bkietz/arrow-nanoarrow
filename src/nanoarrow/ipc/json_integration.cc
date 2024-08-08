// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdlib>
#include <fstream>

#include <nanoarrow/nanoarrow_ipc.hpp>
#include <nanoarrow/nanoarrow_testing.hpp>

struct Table {
  nanoarrow::UniqueSchema schema;
  std::vector<nanoarrow::UniqueArray> arrays;
};

std::string GetEnv(char const* name) {
  if (char const* val = std::getenv(name)) {
    return val;
  }
  return "";
}

std::string ReadFileIntoString(std::string const& path) {
  std::ifstream stream{path};
  std::string contents(static_cast<char>(stream.seekg(0, std::ios_base::end).tellg()),
                       '\0');
  stream.seekg(0).read(contents.data(), contents.size());
  return contents;
}

ArrowErrorCode ReadTableFromArrayStream(struct ArrowArrayStream* stream, Table* table,
                                        struct ArrowError* error) {
  NANOARROW_RETURN_NOT_OK(ArrowArrayStreamGetSchema(stream, table->schema.get(), error));

  while (true) {
    nanoarrow::UniqueArray array;
    NANOARROW_RETURN_NOT_OK(ArrowArrayStreamGetNext(stream, array.get(), error));
    if (array->release == nullptr) {
      break;
    }
    table->arrays.push_back(std::move(array));
  }

  return NANOARROW_OK;
}

ArrowErrorCode ReadTableFromJson(std::string const& json, Table* table,
                                 struct ArrowError* error) {
  nanoarrow::testing::TestingJSONReader reader;
  nanoarrow::UniqueArrayStream array_stream;
  NANOARROW_RETURN_NOT_OK(reader.ReadDataFile(ReadFileIntoString(GetEnv("JSON_PATH")),
                                              array_stream.get(), reader.kNumBatchReadAll,
                                              error));
  return ReadTableFromArrayStream(array_stream.get(), table, error);
}

ArrowErrorCode ReadTableFromIpcFile(std::string const& path, Table* table,
                                    struct ArrowError* error) {
  // TODO when we can read an IPC file, do that
  return EINVAL;
}

ArrowErrorCode WriteTableToIpcFile(std::string const& path, Table const& table,
                                   struct ArrowError* error) {
  // TODO when we can write an IPC file, do that
  return EINVAL;
}

ArrowErrorCode Validate(struct ArrowError* error) {
  Table json_table;
  NANOARROW_RETURN_NOT_OK(
      ReadTableFromJson(ReadFileIntoString(GetEnv("JSON_PATH")), &json_table, error));

  Table arrow_table;
  NANOARROW_RETURN_NOT_OK(
      ReadTableFromIpcFile(GetEnv("ARROW_PATH"), &arrow_table, error));

  // TODO when we can compare schemas and arrays, do that
  return EINVAL;
}

ArrowErrorCode JsonToArrow(struct ArrowError* error) {
  Table table;
  NANOARROW_RETURN_NOT_OK(
      ReadTableFromJson(ReadFileIntoString(GetEnv("JSON_PATH")), &table, error));
  return WriteTableToIpcFile(GetEnv("ARROW_PATH"), table, error);
}

ArrowErrorCode StreamToFile(struct ArrowError* error) {
  // wrap stdin into ArrowIpcInputStream
  nanoarrow::ipc::UniqueInputStream input_stream;
  NANOARROW_RETURN_NOT_OK_WITH_ERROR(
      ArrowIpcInputStreamInitFile(input_stream.get(), stdin, /*close_on_release=*/true),
      error);

  nanoarrow::UniqueArrayStream array_stream;
  NANOARROW_RETURN_NOT_OK_WITH_ERROR(
      ArrowIpcArrayStreamReaderInit(array_stream.get(), input_stream.get(),
                                    /*options=*/nullptr),
      error);

  Table table;
  NANOARROW_RETURN_NOT_OK(ReadTableFromArrayStream(array_stream.get(), &table, error));
  return WriteTableToIpcFile(GetEnv("FILE_PATH"), table, error);
}

ArrowErrorCode FileToStream(struct ArrowError* error) {
  Table table;
  NANOARROW_RETURN_NOT_OK(ReadTableFromIpcFile(GetEnv("FILE_PATH"), &table, error));

  // wrap stdout into ArrowIpcOutputStream
  nanoarrow::ipc::UniqueOutputStream output_stream;
  NANOARROW_RETURN_NOT_OK_WITH_ERROR(
      ArrowIpcOutputStreamInitFile(output_stream.get(), stdout,
                                   /*close_on_release=*/true),
      error);

  nanoarrow::ipc::UniqueWriter writer;
  NANOARROW_RETURN_NOT_OK_WITH_ERROR(
      ArrowIpcWriterInit(writer.get(), output_stream.get()), error);

  NANOARROW_RETURN_NOT_OK(
      ArrowIpcWriterWriteSchema(writer.get(), table.schema.get(), error));

  nanoarrow::UniqueArrayView array_view;
  NANOARROW_RETURN_NOT_OK(
      ArrowArrayViewInitFromSchema(array_view.get(), table.schema.get(), error));
  for (const auto& array : table.arrays) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayViewSetArray(array_view.get(), array.get(), error));
    NANOARROW_RETURN_NOT_OK(
        ArrowIpcWriterWriteArrayView(writer.get(), array_view.get(), error));
  }

  return ArrowIpcWriterWriteArrayView(writer.get(), nullptr, error);
}

int main() try {
  std::string command = GetEnv("COMMAND");

  ArrowErrorCode error_code;
  struct ArrowError error;
  if (command == "VALIDATE") {
    error_code = Validate(&error);
  } else if (command == "JSON_TO_ARROW") {
    error_code = JsonToArrow(&error);
  } else if (command == "STREAM_TO_FILE") {
    error_code = StreamToFile(&error);
  } else if (command == "FILE_TO_STREAM") {
    error_code = FileToStream(&error);
  } else {
    std::cerr << "Invalid command " << command << std::endl;
    return EINVAL;
  }

  std::cerr << "Command " << command << " failed: " << error.message << std::endl;
  return error_code;
} catch (std::exception const& e) {
  std::cerr << "Uncaught exception: " << e.what() << std::endl;
  return 1;
}
