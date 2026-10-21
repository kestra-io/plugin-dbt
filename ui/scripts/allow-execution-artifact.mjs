// artifact-sdk 0.2.2 rejects slot names it does not know, and `execution-artifact/<id>` (kestra-io/kestra#21611)
// is not released yet. Lets the build accept it. Delete this script once the SDK ships the slot.
import { readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";

const file = join(dirname(createRequire(import.meta.url).resolve("@kestra-io/artifact-sdk/package.json")), "dist/vite.config.mjs");
const check = "if (!(module.slotName in KnownSlotsPropNames))";
const allowed = 'if (!module.slotName.startsWith("execution-artifact/") && !(module.slotName in KnownSlotsPropNames))';

const source = readFileSync(file, "utf8");
if (source.includes(allowed)) process.exit(0);
if (!source.includes(check)) {
    throw new Error(`artifact-sdk changed, ${file} has no slot-name check to relax: remove scripts/allow-execution-artifact.mjs if the SDK now knows execution-artifact.`);
}
writeFileSync(file, source.replace(check, allowed));
