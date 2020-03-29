/* Copyright 2015 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#pragma once 

#include <sstream>
#include "status.h"
#include "absl/strings/str_cat.h"

namespace errors {

#define ERR_PREDICT_TRUE(condition) __builtin_expect(static_cast<bool>(condition), 1)
#define ERR_PREDICT_FALSE(condition) __builtin_expect(static_cast<bool>(condition), 0)
typedef errors::Code Code;

namespace internal {

// The DECLARE_ERROR macro below only supports types that can be converted
// into StrCat's AlphaNum. For the other types we rely on a slower path
// through std::stringstream. To add support of a new type, it is enough to
// make sure there is an operator<<() for it:
//
//   std::ostream& operator<<(std::ostream& os, const MyType& foo) {
//     os << foo.ToString();
//     return os;
//   }
// Eventually absl::strings will have native support for this and we will be
// able to completely remove PrepareForStrCat().
template <typename T>
typename std::enable_if<!std::is_convertible<T, absl::AlphaNum>::value,
                        std::string>::type
PrepareForStrCat(const T& t) {
  std::stringstream ss;
  ss << t;
  return ss.str();
}
inline const absl::AlphaNum& PrepareForStrCat(const absl::AlphaNum& a) {
  return a;
}

}  // namespace internal

// Append some context to an error message.  Each time we append
// context put it on a new line, since it is possible for there
// to be several layers of additional context.
template <typename... Args>
void AppendToMessage(::errors::Status* status, Args... args) {
  *status = ::errors::Status(
      status->code(),
      absl::StrCat(status->error_message(), "\n\t", args...));
}

// For propagating errors when calling a function.
#define ERR_RETURN_IF_ERROR(...)                          \
  do {                                                   \
    const ::errors::Status _status = (__VA_ARGS__);  \
    if (ERR_PREDICT_FALSE(!_status.ok())) return _status; \
  } while (0)

#define ERR_RETURN_WITH_CONTEXT_IF_ERROR(expr, ...)                  \
  do {                                                              \
    ::errors::Status _status = (expr);                          \
    if (ERR_PREDICT_FALSE(!_status.ok())) {                          \
      ::errors::errors::AppendToMessage(&_status, __VA_ARGS__); \
      return _status;                                               \
    }                                                               \
  } while (0)

// Convenience functions for generating and using error status.
// Example usage:
//   status.Update(errors::InvalidArgument("The ", foo, " isn't right."));
//   if (errors::IsInvalidArgument(status)) { ... }
//   switch (status.code()) { case error::INVALID_ARGUMENT: ... }

#define DECLARE_ERROR(FUNC, CONST)                                       \
  template <typename... Args>                                            \
  ::errors::Status FUNC(Args... args) {                              \
    return ::errors::Status(                                         \
        ::errors::CONST,                                      \
        absl::StrCat(                                   \
            ::errors::internal::PrepareForStrCat(args)...)); \
  }                                                                      \
  inline bool Is##FUNC(const ::errors::Status& status) {             \
    return status.code() == ::errors::CONST;                  \
  }

//DECLARE_ERROR(Cancelled, CANCELLED)
DECLARE_ERROR(InvalidArgument, INVALID_ARGUMENT)
DECLARE_ERROR(NotFound, NOT_FOUND)
//DECLARE_ERROR(AlreadyExists, ALREADY_EXISTS)
DECLARE_ERROR(ResourceExhausted, RESOURCE_EXHAUSTED)
DECLARE_ERROR(Unavailable, UNAVAILABLE)
//DECLARE_ERROR(FailedPrecondition, FAILED_PRECONDITION)
DECLARE_ERROR(OutOfRange, OUT_OF_RANGE)
//DECLARE_ERROR(Unimplemented, UNIMPLEMENTED)
DECLARE_ERROR(Internal, INTERNAL)
//DECLARE_ERROR(Aborted, ABORTED)
//DECLARE_ERROR(DeadlineExceeded, DEADLINE_EXCEEDED)
//DECLARE_ERROR(DataLoss, DATA_LOSS)
DECLARE_ERROR(Unknown, UNKNOWN)
//DECLARE_ERROR(PermissionDenied, PERMISSION_DENIED)
//DECLARE_ERROR(Unauthenticated, UNAUTHENTICATED)

#undef DECLARE_ERROR

// The CanonicalCode() for non-errors.
using errors::OK;

}  // namespace errors

