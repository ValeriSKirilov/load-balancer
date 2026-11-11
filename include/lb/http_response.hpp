#pragma once

#include <map>
#include <string>

struct HttpResponse {
  int status_code;
  std::string status_text;
  std::map<std::string, std::string> headers;
  std::string body;

  std::string to_raw_string() const;
};