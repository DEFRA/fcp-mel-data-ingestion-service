"use strict";

const { TextDecoder } = require("node:util");

function parseCsv(buffer) {
  const fail = code => {
    const error = new Error(code);
    error.pilotCode = code;
    throw error;
  };

  let text;

  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(buffer);
  } catch {
    fail("CSV_NOT_UTF8");
  }

  text = text.replace(/^\uFEFF/, "");

  if (text.includes("\0")) fail("CSV_CONTAINS_NUL");

  const records = [];
  let row = [];
  let field = "";
  let quoted = false;
  let afterQuote = false;
  let touched = false;
  let blankLines = 0;

  function endField() {
    row.push(field);
    field = "";
    afterQuote = false;
  }

  function endRow() {
    if (!touched && row.length === 0 && field === "") {
      blankLines++;
    } else {
      endField();
      records.push(row);

      if (records.length > 100001) {
        fail("CSV_TOO_MANY_ROWS");
      }
    }

    row = [];
    field = "";
    afterQuote = false;
    touched = false;
  }

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];

    if (quoted) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          quoted = false;
          afterQuote = true;
        }
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === "\r" || ch === "\n") {
      endRow();
      if (ch === "\r" && text[i + 1] === "\n") i++;
      continue;
    }

    if (ch === ",") {
      touched = true;
      endField();
      continue;
    }

    if (afterQuote) fail("CSV_CHARACTERS_AFTER_QUOTE");

    if (ch === '"') {
      if (field !== "") fail("CSV_UNEXPECTED_QUOTE");
      quoted = true;
      touched = true;
      continue;
    }

    field += ch;
    touched = true;
  }

  if (quoted) fail("CSV_UNCLOSED_QUOTE");

  if (touched || row.length > 0 || field !== "") {
    endRow();
  }

  if (records.length < 2) fail("CSV_HAS_NO_DATA_ROWS");

  const headers = records.shift();

  // More than one column is expected for this MEL pilot.
  if (headers.length < 2) fail("CSV_SEPARATOR_OR_HEADER_UNEXPECTED");

  if (
    headers.some(name =>
      name.trim() === "" ||
      name.includes(".") ||
      name.startsWith("$")
    )
  ) {
    fail("CSV_UNSUPPORTED_HEADER");
  }

  if (new Set(headers.map(name => name.trim())).size !== headers.length) {
    fail("CSV_DUPLICATE_HEADERS");
  }

  if (records.some(values => values.length !== headers.length)) {
    fail("CSV_ROW_WIDTH_MISMATCH");
  }

  const rows = records.map(values =>
    Object.fromEntries(headers.map((name, index) => [name, values[index]]))
  );

  // Conservative record-size guard before any database writes.
  if (rows.some(value => Buffer.byteLength(JSON.stringify(value)) > 1000000)) {
    fail("CSV_RECORD_TOO_LARGE");
  }

  return { headers, rows, blankLines };
}

module.exports = { parseCsv };