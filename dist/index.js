#!/usr/bin/env node
import chalk from "chalk";
import fs from "fs";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const args = process.argv.slice(2);
const command = args.join(" ");
const commandsToExecute = {
    generate: "generate",
};
const executeFile = async (fileName) => {
    const filePath = path.join(__dirname, fileName);
    if (!fs.existsSync(filePath)) {
        console.error(chalk.red(`File '${fileName}' not found.`));
        process.exit(1);
    }
    try {
        const module = await import(pathToFileURL(filePath).toString());
        if (module.default) {
            module.default();
        }
    }
    catch (error) {
        console.error("Error executing file:", error);
    }
};
const main = async () => {
    if (args.length === 0) {
        console.log("No command provided.");
        return;
    }
    if (commandsToExecute.generate === command) {
        await executeFile("generate.js");
    }
    else {
        console.log(chalk.red("Invalid command. Use: npx ppo generate"));
    }
};
main().catch((error) => {
    console.error("Unhandled error in main function:", error);
});
