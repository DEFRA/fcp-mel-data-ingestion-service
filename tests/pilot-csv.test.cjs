const assert = require("node:assert/strict");
const path = require("node:path");

const scriptDirectory =
  process.env.MEL_SCRIPT_DIR || path.resolve(process.cwd(), "scripts");

const { parseCsv } = require(
  path.join(scriptDirectory, "pilot-csv.cjs")
);

function parse(text) {
  return parseCsv(Buffer.from(text, "utf8"));
}

assert.deepEqual(
  parse("id,value\n00123,0\n").rows,
  [{ id: "00123", value: "0" }]
);

assert.deepEqual(
  parse('id,note\n1,"a,b"\n').rows,
  [{ id: "1", note: "a,b" }]
);

assert.equal(
  parse('id,note\n1,"line one\nline two"\n').rows[0].note,
  "line one\nline two"
);

assert.equal(
  parse('id,note\n1,"say ""hello"""\n').rows[0].note,
  'say "hello"'
);

assert.equal(parse("\uFEFFid,value\r\n1,2\r\n").rows.length, 1);
assert.equal(parse("id,value\n\n1,2\n").blankLines, 1);

assert.throws(() => parse("id,id\n1,2\n"), /CSV_DUPLICATE_HEADERS/);
assert.throws(() => parse("id,value\n1,2,3\n"), /CSV_ROW_WIDTH_MISMATCH/);
assert.throws(() => parse('id,value\n1,"abc'), /CSV_UNCLOSED_QUOTE/);
assert.throws(() => parse("id;value\n1;2\n"), /CSV_SEPARATOR_OR_HEADER_UNEXPECTED/);
assert.throws(() => parse("id,value\n"), /CSV_HAS_NO_DATA_ROWS/);

console.log("CSV parser tests passed.");