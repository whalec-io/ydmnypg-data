function isDict(v) {
  return typeof v === "object" && v !== null && !Array.isArray(v) && !(v instanceof Date);
}

function resolveNested(obj, path) {
  return path.split(".").reduce((acc, k) => (acc == null ? acc : acc[k]), obj);
}

function pushPlaceholder(ctx, value) {
  ctx.values.push(value);
  return ctx.dialect === "postgres" ? `$${ctx.values.length}` : "?";
}

function unescapeCdata(s) {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"');
}

export function convertChildren(children, param, namespace, mapper, ctx) {
  if (param == null) param = {};
  if (!isDict(param)) throw new Error("Parameter argument should be Key-Value type or Null.");

  if (children.type === "text") {
    return convertParameters(children, param, ctx);
  }
  if (children.type !== "tag") return "";

  switch (children.name.toLowerCase()) {
    case "if":
      return convertIf(children, param, namespace, mapper, ctx);
    case "choose":
      return convertChoose(children, param, namespace, mapper, ctx);
    case "trim":
    case "where":
      return convertTrimWhere(children, param, namespace, mapper, ctx);
    case "set":
      return convertSet(children, param, namespace, mapper, ctx);
    case "foreach":
      return convertForeach(children, param, namespace, mapper, ctx);
    case "bind":
      convertBind(children, param);
      return "";
    case "include":
      return convertInclude(children, param, namespace, mapper, ctx);
    default:
      throw new Error("XML is not well-formed character or markup. Consider using CDATA section.");
  }
}

function convertParameters(children, param, ctx) {
  let s = children.content || "";

  // ${...} → literal substitution (used for table/column names)
  s = s.replace(/\$\{([^}]+)\}/g, (_m, path) => {
    const v = resolveNested(param, path);
    return v == null ? "" : String(v);
  });

  // #{...} → parameterized placeholder, push value in document order
  s = s.replace(/#\{([^}]+)\}/g, (_m, path) => {
    const v = resolveNested(param, path);
    return pushPlaceholder(ctx, v == null ? null : v);
  });

  return unescapeCdata(s);
}

function buildEvalString(rawTest, param) {
  let evalString = replaceEvalString(rawTest, param);
  evalString = evalString.replace(/ and /gi, " && ");
  evalString = evalString.replace(/ or /gi, " || ");
  evalString = evalString.replace(/==/g, "===");
  evalString = evalString.replace(/!=/g, "!==");
  return evalString;
}

function convertIf(children, param, namespace, mapper, ctx) {
  let evalString;
  try {
    evalString = buildEvalString(children.attrs.test, param);
  } catch (_) {
    throw new Error("Error occurred during convert <if> element.");
  }
  try {
    // eslint-disable-next-line no-eval
    if (eval(evalString)) {
      let out = "";
      for (const c of children.children || []) {
        out += convertChildren(c, param, namespace, mapper, ctx);
      }
      return out;
    }
  } catch (_) {
    return "";
  }
  return "";
}

function convertChoose(children, param, namespace, mapper, ctx) {
  for (const wc of children.children || []) {
    if (wc.type === "tag" && wc.name.toLowerCase() === "when") {
      let evalString;
      try {
        evalString = buildEvalString(wc.attrs.test, param);
      } catch (_) {
        continue;
      }
      try {
        // eslint-disable-next-line no-eval
        if (eval(evalString)) {
          let out = "";
          for (const c of wc.children || []) {
            out += convertChildren(c, param, namespace, mapper, ctx);
          }
          return out;
        }
      } catch (_) {
        continue;
      }
    } else if (wc.type === "tag" && wc.name.toLowerCase() === "otherwise") {
      let out = "";
      for (const c of wc.children || []) {
        out += convertChildren(c, param, namespace, mapper, ctx);
      }
      return out;
    }
  }
  return "";
}

function convertTrimWhere(children, param, namespace, mapper, ctx) {
  let prefix, prefixOverrides, globalSet;
  switch (children.name.toLowerCase()) {
    case "trim":
      prefix = children.attrs.prefix;
      prefixOverrides = children.attrs.prefixOverrides;
      globalSet = "g";
      break;
    case "where":
      prefix = "WHERE";
      prefixOverrides = "and|or";
      globalSet = "gi";
      break;
    default:
      throw new Error("Error occurred during convert <trim/where> element.");
  }

  let out = "";
  for (const c of children.children || []) {
    out += convertChildren(c, param, namespace, mapper, ctx);
  }

  out = out.replace(new RegExp(`(^)([\\s]*?)(${prefixOverrides})`, globalSet), "");
  if (children.name.toLowerCase() !== "trim") {
    out = out.replace(new RegExp(`(${prefixOverrides})([\\s]*?)($)`, globalSet), "");
  }

  if (/[a-zA-Z]/.test(out)) {
    out = `${prefix} ${out}`;
  }

  if (children.name.toLowerCase() !== "where") {
    out = out.replace(/(,)([\s]*?)(where)/gi, " WHERE ");
  }

  return out;
}

function convertSet(children, param, namespace, mapper, ctx) {
  let out = "";
  for (const c of children.children || []) {
    out += convertChildren(c, param, namespace, mapper, ctx);
  }
  out = out.replace(/(,)(,|\s){2,}/g, ",\n");
  out = out.replace(/(^)([\s]*?)(,)/g, "");
  out = out.replace(/(,)([\s]*?)($)/g, "");
  return ` SET ${out}`;
}

function convertForeach(children, param, namespace, mapper, ctx) {
  const collection = resolveNested(param, children.attrs.collection);
  if (collection == null) return "";

  const item = children.attrs.item;
  const open = children.attrs.open ?? "";
  const close = children.attrs.close ?? "";
  const separator = children.attrs.separator ?? "";

  const parts = [];
  for (const coll of collection) {
    const foreachParam = { ...param, [item]: coll };
    let text = "";
    for (const c of children.children || []) {
      const ft = convertChildren(c, foreachParam, namespace, mapper, ctx).replace(/^\s*$/g, "");
      if (ft && ft.length > 0) text += ft;
    }
    if (text.length > 0) parts.push(text);
  }

  return `${open}${parts.join(separator)}${close}`;
}

function convertBind(children, param) {
  const evalString = replaceEvalString(children.attrs.value, param);
  // eslint-disable-next-line no-eval
  param[children.attrs.name] = eval(evalString);
  return param;
}

function convertInclude(children, param, namespace, mapper, ctx) {
  for (const c of children.children || []) {
    if (c.type === "tag" && c.name === "property") {
      param[c.attrs.name] = c.attrs.value;
    }
  }

  // refid may itself contain ${...} substitutions
  let refid = children.attrs.refid;
  refid = refid.replace(/\$\{([^}]+)\}/g, (_m, path) => {
    const v = resolveNested(param, path);
    return v == null ? "" : String(v);
  });

  const ref = mapper[namespace]?.[refid];
  if (!ref) throw new Error(`Error occurred during convert 'refid' [${refid}] in <include>.`);

  let out = "";
  for (const c of ref) {
    out += convertChildren(c, param, namespace, mapper, ctx);
  }
  return out;
}

function replaceEvalString(evalString, param) {
  const keys = Object.keys(param);
  for (const k of keys) {
    let regex;
    if (isDict(param[k])) {
      regex = new RegExp(`(^|[^a-zA-Z0-9])(${k}\\.)([a-zA-Z0-9]+)`, "g");
      evalString = evalString.replace(regex, "$1 param.$2$3");
    } else {
      regex = new RegExp(`(^|[^a-zA-Z0-9])(${k})($|[^a-zA-Z0-9])`, "g");
      evalString = evalString.replace(regex, "$1 param.$2 $3");
    }
  }
  return evalString;
}
