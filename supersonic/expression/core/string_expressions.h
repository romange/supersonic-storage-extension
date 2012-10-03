// Copyright 2010 Google Inc.  All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//
// Expressions on strings.

#ifndef SUPERSONIC_EXPRESSION_CORE_STRING_EXPRESSIONS_H_
#define SUPERSONIC_EXPRESSION_CORE_STRING_EXPRESSIONS_H_

#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

// Creates an expression that will convert any expression to VARCHAR.
class Expression;
class ExpressionList;

const Expression* ToString(const Expression* arg);

// The ParseString expressions are in elementary_expressions.h.

// Concatenates the specified input. Converts all arguments to VARCHAR if
// they are not already.
const Expression* Concat(const ExpressionList* arguments);

// Concatenates the specified input, using the specified string as a separator.
// Converts all arguments to VARCHAR if they are not already.
//
// Currently not implemented.
const Expression* ConcatWithSeparator(const StringPiece& separator,
                                      const ExpressionList* arguments);

// Computes the length of the specified string.
// Returns NULL if the string is NULL.
const Expression* Length(const Expression* str);

// Removes white spaces from the left side of the specified string.
// Returns NULL if the string is NULL.
const Expression* Ltrim(const Expression* str);

// Removes white spaces from the right side of the specified string.
// Returns NULL if the string is NULL.
const Expression* Rtrim(const Expression* str);

// Removes white spaces from both sides of the specified string.
// Returns NULL if the string is NULL.
const Expression* Trim(const Expression* str);

// Converts the specified string to upper case or lower case, respectively.
// Returns NULL if the string is NULL.
const Expression* ToUpper(const Expression* str);
const Expression* ToLower(const Expression* str);

// Returns a substring starting at the position determined by the 'pos'
// argument. Returns NULL if either the string or the pos argument evaluate
// to NULL.
// Non-positive arguments are interpreted by the "from the end" semantics, see
// below.
const Expression* TrailingSubstring(const Expression* str,
                                    const Expression* pos);

// Returns a substring starting at the position determined by the 'pos'
// argument, and at most 'length' bytes long. Returns NULL if either the
// string, pos, or length arguments evaluate to NULL.
// One-based (i.e., Substring("Cow", 2, 2) = "ow").
// Negative length is interpreted as zero.
// Negative pos is interpreted as "count from the end" (python-like semantics),
// thus Substring("Cow", -1, 1) = "w".
// Substring(str, 0, len) always returns an empty string (as in MySQL).
const Expression* Substring(const Expression* str,
                            const Expression* pos,
                            const Expression* length);

// Returns the first index (1-based) that is a beginning of needle in haystack,
// or zero if needle does not appear in haystack.
const Expression* StringOffset(const Expression* const haystack,
                               const Expression* const needle);

// Returns true if needle appears in haystack.
const Expression* StringContains(const Expression* const haystack,
                                 const Expression* const needle);

// Case insensitive variant of StringContains expression.
// The current implementation is not very efficient yet (uses conversion to
// lower string).
const Expression* StringContainsCI(const Expression* const haystack,
                                   const Expression* const needle);

// Performs partial regular expression matching, using RE2, on the specified
// string argument. Returns true if matched, false if not matched, NULL if
// the argument is NULL.
//
// Note: the argument order contravenes the standard SuperSonic order of
// "variable arguments at the end".
const Expression* RegexpPartialMatch(const Expression* str,
                                     const StringPiece& pattern);

// Performs full regular expression matching, using RE2, on the specified
// string argument. Returns true if matched, false if not matched, NULL if
// the argument is NULL.
//
// Note: the argument order contravenes the standard SuperSonic order of
// "variable arguments at the end".
const Expression* RegexpFullMatch(const Expression* str,
                                  const StringPiece& pattern);

// Replace all occurences of "needle" in "haystack" with "substitute".
// Needle can be a regular expression.
const Expression* RegexpReplace(const Expression* haystack,
                                const StringPiece& needle,
                                const Expression* substitute);

// Replace all occurences of "needle" in "haystack" with "substitute".
// Needle is treated as a string (no regexps).
const Expression* StringReplace(const Expression* haystack,
                                const Expression* needle,
                                const Expression* substitute);

// Replace the first match of "pattern" in "str" with "rewrite". Within
// "rewrite", backslash-escaped digits (\1 to \9) can be used to insert text
// matching corresponding parenthesized group from the pattern.  \0 in
// "rewrite" refers to the entire matching text.
// If not matched, or if the argument is NULL, results in NULL.
//
// Currently not implemented.
const Expression* RegexpRewrite(const Expression* str,
                                const StringPiece& pattern,
                                const StringPiece& rewrite);

// Return the first substring of "str" matching "pattern". If "pattern" cannot
// be matched into substring, returns NULL.
const Expression* RegexpExtract(const Expression* str,
                                const StringPiece& pattern);

// Replace the first match of "pattern" in "str" with "rewrite". Within
// "rewrite", backslash-escaped digits (\1 to \9) can be used to insert text
// matching corresponding parenthesized group from the pattern.  \0 in
// "rewrite" refers to the entire matching text.
// If the argument is NULL, results in NULL. If the argument is not NULL but
// the pattern did not match, returns the default value.
//
// Currently not implemented.
const Expression* RegexpRewrite(const Expression* str,
                                const Expression* default_value,
                                const StringPiece& pattern,
                                const StringPiece& rewrite);

}  // namespace supersonic

#endif  // SUPERSONIC_EXPRESSION_CORE_STRING_EXPRESSIONS_H_
