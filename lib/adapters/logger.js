import createDebug from "debug";

const DebugLevel = {
  LOG: 1,
  DEBUG: 2,
  INFO: 3,
  TRACE: 4,
  WARN: 5,
  ERROR: 6,
  FATAL: 7,
  NONE: 9,
};

const LEVEL_NAMES = ["log", "debug", "info", "trace", "warn", "error", "fatal"];

function createTimer(prefix) {
  let count = 0;
  const timings = new Map();

  return {
    start() {
      const id = `${prefix}.${count++}`;
      timings.set(id, Date.now());
      return id;
    },
    end(id) {
      const start = timings.get(id);
      timings.delete(id);
      return start !== undefined ? Date.now() - start : 0;
    },
    clear(id) {
      timings.delete(id);
    },
  };
}

class Logger {
  constructor(namespace, externalLogger) {
    this.namespace = namespace;
    this.level = DebugLevel.NONE;
    this.initializing = false;

    this.queryTimer = createTimer("Q");
    this.transactionTimer = createTimer("T");
    this.taskTimer = createTimer("C");

    if (externalLogger) {
      this.ext = externalLogger;
      this.level = DebugLevel.TRACE;
    } else {
      const env = process.env.DEBUG || "";
      for (let i = 0; i < LEVEL_NAMES.length; i++) {
        if (env.includes(`${namespace}:${LEVEL_NAMES[i]}`)) {
          this.level = i + 1;
          break;
        }
      }
      if (
        this.level === DebugLevel.NONE &&
        (env.includes(namespace) || env.includes("dao:*"))
      ) {
        this.level = DebugLevel.TRACE;
      }
      this.writer = createDebug(namespace);
      this.writer.enabled = this.level < DebugLevel.NONE;
    }
  }

  write(levelName, ...args) {
    if (this.ext) {
      this.ext[levelName]
        ? this.ext[levelName](...args)
        : this.ext.info(...args);
    } else if (this.writer && this.writer.enabled) {
      this.writer(...args);
    }
  }

  log(...msg) {
    if (this.level <= DebugLevel.LOG) this.write("debug", ...msg);
  }

  debug(...msg) {
    if (this.level <= DebugLevel.DEBUG) this.write("debug", ...msg);
  }

  info(...msg) {
    if (this.level <= DebugLevel.INFO) this.write("info", ...msg);
  }

  trace(...msg) {
    if (this.level <= DebugLevel.TRACE) this.write("trace", ...msg);
  }

  warn(...msg) {
    if (this.level <= DebugLevel.WARN) this.write("warn", ...msg);
  }

  error(...msg) {
    if (this.level <= DebugLevel.ERROR) this.write("error", ...msg);
  }

  queryStart({ sql, values }) {
    const id = this.queryTimer.start();
    if (
      this.level <= DebugLevel.INFO ||
      (!this.initializing && this.level <= DebugLevel.TRACE)
    ) {
      const clean = (sql || "").replace(/[\n\t]+/g, " ").replace(/ +/g, " ");
      this.write(
        "debug",
        "Query Start",
        id,
        "\n  ",
        clean,
        JSON.stringify(values),
      );
    }
    return id;
  }

  queryEnd(id, meta = {}) {
    if (this.level <= DebugLevel.INFO) {
      const elapsed = this.queryTimer.end(id);
      if (meta.error) {
        this.write(
          "error",
          "Query Error",
          id,
          elapsed + "ms",
          meta.error.message,
        );
      } else {
        this.write("debug", "Query End", id, elapsed + "ms");
      }
    } else {
      this.queryTimer.clear(id);
    }
  }

  transactionStart() {
    const id = this.transactionTimer.start();
    if (this.level <= DebugLevel.DEBUG)
      this.write("debug", "Transaction Start", id);
    return id;
  }

  transactionEnd(id, action) {
    if (this.level <= DebugLevel.DEBUG) {
      const elapsed = this.transactionTimer.end(id);
      this.write("debug", "Transaction End", id, action, elapsed + "ms");
    } else {
      this.transactionTimer.clear(id);
    }
  }

  taskStart() {
    const id = this.taskTimer.start();
    if (this.level <= DebugLevel.DEBUG) this.write("debug", "Task Start", id);
    return id;
  }

  taskEnd(id) {
    if (this.level <= DebugLevel.DEBUG) {
      const elapsed = this.taskTimer.end(id);
      this.write("debug", "Task End", id, elapsed + "ms");
    } else {
      this.taskTimer.clear(id);
    }
  }
}

export { Logger, DebugLevel };
