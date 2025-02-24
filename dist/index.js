#!/usr/bin/env node
import chalk from "chalk";
import fs from "fs";
import path from "path";
import { pathToFileURL } from "url";
import { getFileMeta } from "./utils.js";
const { __dirname } = getFileMeta();
const args = process.argv.slice(2);
const commandsToExecute = {
  generate: "generate",
  init: "init",
};
const executeFile = async (fileName, extraArgs = []) => {
  const filePath = path.join(__dirname, fileName);
  if (!fs.existsSync(filePath)) {
    console.error(chalk.red(`File '${fileName}' not found.`));
    process.exit(1);
  }
  try {
    const module = await import(pathToFileURL(filePath).toString());
    if (module.default) {
      module.default(extraArgs); // Pass extraArgs (even if empty)
    }
  } catch (error) {
    console.error("Error executing file:", error);
  }
};
const main = async () => {
  if (args.length === 0) {
    console.log("No command provided.");
    return;
  }
  const commandName = args[0]; // First argument is the command (e.g., "init" or "generate")
  const extraArgs = args.slice(1); // Capture any additional arguments (e.g., "--prisma-php")
  if (commandsToExecute.generate === commandName) {
    await executeFile("generate.js", extraArgs);
  } else if (commandsToExecute.init === commandName) {
    await executeFile("init.js", extraArgs);
  } else {
    console.log(chalk.red("Invalid command. Use: npx ppo generate"));
  }
};
main().catch((error) => {
  console.error("Unhandled error in main function:", error);
});
