#include "lb/waf.hpp"

#include <cctype>
#include <iostream>
#include <regex>

namespace {

int hex_char_to_int(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return 0;
}

bool starts_with_icase(std::string_view text, std::string_view prefix) {
  if (text.size() < prefix.size()) return false;
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(text[i])) !=
        std::tolower(static_cast<unsigned char>(prefix[i]))) {
      return false;
    }
  }
  return true;
}

std::string strip_accept_headers(const std::string& request) {
  std::string result;
  result.reserve(request.size());

  size_t pos = 0;
  bool is_request_line = true;

  while (pos < request.size()) {
    size_t eol = request.find("\r\n", pos);
    size_t next = (eol == std::string::npos) ? request.size() : eol + 2;
    std::string_view line(request.data() + pos, next - pos);

    if (line == "\r\n") {
      result.append(request, pos, std::string::npos);
      break;
    }

    if (is_request_line || !starts_with_icase(line, "accept")) {
      result.append(line);
    }
    is_request_line = false;
    pos = next;
  }

  return result;
}
}  // namespace

// TODO

std::string url_decode_once(std::string_view input) {
  std::string result;
  for (size_t i = 0; i < input.size(); ++i) {
    if (input[i] == '+') {
      result.push_back(' ');
    } else if (input[i] == '%' && i + 2 < input.size() &&
               std::isxdigit(static_cast<unsigned char>(input[i + 1])) &&
               std::isxdigit(static_cast<unsigned char>(input[i + 2]))) {
      int high = hex_char_to_int(input[i + 1]);
      int low = hex_char_to_int(input[i + 2]);

      result.push_back(static_cast<char>((high << 4) | low));
      i += 2;
    } else {
      result.push_back(input[i]);
    }
  }

  return result;
}

std::string normalize_payload(std::string_view raw_request) {
  std::string current(raw_request);

  for (int i = 0; i < 3; ++i) {
    std::string next = url_decode_once(current);
    if (next == current) {
      break;
    }
    current = std::move(next);
  }
  return current;
}

bool inspect_request(const std::string& request) {
  std::string normalized_request =
      normalize_payload(strip_accept_headers(request));

  static const std::regex master_sqli_rule(
      R"((\b(?:OR|AND)\b\s+['"]?(\w+)['"]?\s*=\s*['"]?\2\b)|)"
      R"((\bUNION\b\s+(?:\bALL\b\s+)?\bSELECT\b)|)"
      R"((;\s*(?:DROP|EXEC|ALTER|TRUNCATE)\b)|)"
      R"((\b(?:SLEEP|pg_sleep|WAITFOR\s+DELAY)\b)|)"
      R"((['";]\s*(?:--|#))|(\/\*[\s\S]*?\*\/)|)"
      R"((<\s*script\b|javascript\s*:)|)"
      R"((\.\.\/|\.\.\\|\/etc\/passwd))",
      std::regex::icase | std::regex::optimize);

  std::smatch match;

  if (std::regex_search(normalized_request, match, master_sqli_rule)) {
    std::cerr << "[BLOCKED] Full HTTP string: " << request << "\n";
    std::cerr << "  -> Exact offending trigger (match[0]): " << match.str(0)
              << "\n";
    std::cerr << "  -> Trigger started at byte offset: " << match.position(0)
              << "\n";

    if (match[1].matched) {
      std::cerr << "  -> Tautology value repeated (match[2]): " << match.str(2)
                << "\n";
    }
    return true;
  }

  return false;
}