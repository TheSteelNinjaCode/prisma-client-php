import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import { getFileMeta } from "./utils.js";
const { __dirname } = getFileMeta();
/**
 * Recursively copies all files and folders from `src` to `dest`,
 * overwriting existing files if they already exist.
 *
 * The `base` parameter represents the original root of the copy.
 * It is used for computing a relative path for cleaner logging.
 */
function copyRecursiveSync(src, dest, base = src) {
  if (!fs.existsSync(src)) {
    console.error(`Source folder does not exist: ${src}`);
    return;
  }
  if (!fs.existsSync(dest)) {
    fs.mkdirSync(dest, { recursive: true });
  }
  const entries = fs.readdirSync(src, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      copyRecursiveSync(srcPath, destPath, base);
    } else {
      // Compute the relative path for logging
      const relative = path.relative(base, srcPath);
      const display = path.join(path.basename(base), relative);
      // Copy the file (this will overwrite if it exists)
      fs.copyFileSync(srcPath, destPath);
      console.log(`Copied file: ${display}`);
    }
  }
}
// Configuration for directories to copy (copying the whole directory recursively)
const directoriesToCopy = [
  {
    srcFolder: path.join(__dirname, "src"),
    destFolder: path.join(process.cwd(), "src"),
  },
  {
    srcFolder: path.join(__dirname, "settings"),
    destFolder: path.join(process.cwd(), "settings"),
  },
  {
    srcFolder: path.join(__dirname, "prisma"),
    destFolder: path.join(process.cwd(), "prisma"),
  },
];
/**
 * Installs specified packages using npm in the current working directory.
 */
function installPackages() {
  const packages = [
    "prisma@^6.4.1",
    "@prisma/client@^6.4.1",
    "@prisma/internals@^6.4.1",
  ];
  const packagesStr = packages.join(" ");
  try {
    console.log(`Installing packages: ${packagesStr}`);
    execSync(`npm install ${packagesStr}`, {
      stdio: "inherit",
      cwd: process.cwd(),
    });
    console.log("Packages installed successfully.");
  } catch (error) {
    console.error("Error installing packages:", error);
  }
}
/**
 * Runs the `npx prisma init` command to initialize Prisma.
 * If the prisma folder already exists, the command is skipped.
 */
function initPrisma() {
  const prismaFolderPath = path.join(process.cwd(), "prisma");
  if (fs.existsSync(prismaFolderPath)) {
    console.warn("Prisma folder already exists. Skipping prisma init.");
    return;
  }
  try {
    console.log("Initializing Prisma...");
    execSync(`npx prisma init`, {
      stdio: "inherit",
      cwd: process.cwd(),
    });
    console.log("Prisma initialized successfully.");
  } catch (error) {
    console.error("Error initializing Prisma:", error);
  }
}
/**
 * Updates the composer.json file by adding "calicastle/cuid": "^2.0.0" to its require section.
 */
async function updateComposerJson(baseDir) {
  const composerJsonPath = path.join(baseDir, "composer.json");
  let composerJson;
  if (fs.existsSync(composerJsonPath)) {
    const composerJsonContent = fs.readFileSync(composerJsonPath, "utf8");
    composerJson = JSON.parse(composerJsonContent);
  } else {
    console.error("composer.json does not exist.");
    return;
  }
  composerJson.require = {
    ...composerJson.require,
    "calicastle/cuid": "^2.0.0",
  };
  fs.writeFileSync(composerJsonPath, JSON.stringify(composerJson, null, 2));
  console.log("composer.json updated successfully.");
}
/**
 * Installs the Composer package "calicastle/cuid": "^2.0.0" using the require command.
 */
function runComposerInstall() {
  try {
    console.log("Installing Composer package calicastle/cuid...");
    execSync(
      `C:\\xampp\\php\\php.exe C:\\ProgramData\\ComposerSetup\\bin\\composer.phar require calicastle/cuid:^2.0.0`,
      {
        stdio: "inherit",
      }
    );
    console.log("Composer package calicastle/cuid installed successfully.");
  } catch (error) {
    console.error("Error installing Composer package calicastle/cuid:", error);
  }
}
/**
 * Main execution flow.
 *
 * If the flag "--prisma-php" is passed, it will update composer.json.
 * Otherwise, it will run Composer install to install the package.
 * Then, it proceeds with npm package installation, Prisma initialization, and file copying.
 */
async function main() {
  const isPrismaPHP = process.argv.includes("--prisma-php");
  if (isPrismaPHP) {
    // Always update composer.json
    await updateComposerJson(process.cwd());
  } else {
    // Install the Composer package "calicastle/cuid": "^2.0.0"
    runComposerInstall();
  }
  installPackages();
  initPrisma();
  directoriesToCopy.forEach((config) => {
    // Pass the srcFolder as the base for computing relative paths in the log
    copyRecursiveSync(config.srcFolder, config.destFolder, config.srcFolder);
  });
  console.log("Finished copying directories.");
}
// Run the main function
main().catch((error) => {
  console.error("Error during execution:", error);
});
