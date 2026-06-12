import fs from "fs";
import HTML from "html-parse-stringify2";
import { convertChildren } from "./convert.js";

const QUERY_TYPES = ["sql", "select", "insert", "update", "delete"];

const mapper = {};

function replaceCdata(rawText) {
  const cdataRegex = /(<!\[CDATA\[)([\s\S]*?)(\]\]>)/g;
  const matches = rawText.match(cdataRegex);
  if (!matches) return rawText;

  for (const match of matches) {
    const m = /(<!\[CDATA\[)([\s\S]*?)(\]\]>)/g.exec(match);
    let cdataText = m[2];
    cdataText = cdataText.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
    rawText = rawText.replace(m[0], cdataText);
  }
  return rawText;
}

function findMapper(node) {
  if (node.type === "tag" && node.name === "mapper") {
    const ns = node.attrs.namespace;
    mapper[ns] = {};
    for (const sql of node.children || []) {
      if (sql.type === "tag" && QUERY_TYPES.includes(sql.name)) {
        mapper[ns][sql.attrs.id] = sql.children;
      }
    }
    return;
  }
  if (node.children?.length) {
    for (const child of node.children) findMapper(child);
  }
}

export function createMapper(xmls) {
  for (const xml of xmls) {
    let parsed;
    try {
      const rawText = replaceCdata(fs.readFileSync(xml).toString());
      parsed = HTML.parse(rawText);
    } catch (_) {
      throw new Error(`Error occured during open XML file [${xml}]`);
    }
    try {
      for (const node of parsed) findMapper(node);
    } catch (_) {
      throw new Error(`Error occured during parse XML file [${xml}]`);
    }
  }
}

export function getStatement(namespace, sqlId, param, options = {}) {
  if (namespace == null) throw new Error("Namespace should not be null.");
  if (mapper[namespace] === undefined) throw new Error(`Namespace [${namespace}] not exists.`);
  if (sqlId == null) throw new Error("SQL ID should not be null.");
  if (mapper[namespace][sqlId] === undefined) throw new Error(`SQL ID [${sqlId}] not exists`);

  const dialect = options.dialect === "postgres" ? "postgres" : "mysql";
  const ctx = { values: [], dialect };

  let sql = "";
  for (const child of mapper[namespace][sqlId]) {
    sql += convertChildren(child, param, namespace, mapper, ctx);
  }

  // Sanity check: any unresolved placeholders left?
  const unresolved = sql.match(/#\{[^}]+\}|\$\{[^}]+\}/g);
  if (unresolved) {
    throw new Error(`Parameter ${unresolved.join(",")} is not converted.`);
  }

  return { sql, values: ctx.values };
}

export function getMapper() {
  return mapper;
}

export default { createMapper, getStatement, getMapper };
