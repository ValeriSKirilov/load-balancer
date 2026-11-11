#include "lb/http_response.hpp"

#include <sstream>

std::string HttpResponse::to_raw_string() const {
  std::ostringstream oss;
  oss << "HTTP/1.1 " << status_code << " " << status_text << "\r\n";
  for (const auto& [key, value] : headers) {
    oss << key << ": " << value << "\r\n";
  }

  oss << "Content-Length: " << body.size() << "\r\n";
  oss << "\r\n";

  oss << body;
  return oss.str();
}