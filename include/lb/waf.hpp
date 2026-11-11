#pragma once

#include <string>
#include <string_view>

std::string url_decode_once(std::string_view input);

std::string normalize_payload(std::string_view raw_request);

bool inspect_request(const std::string& request);