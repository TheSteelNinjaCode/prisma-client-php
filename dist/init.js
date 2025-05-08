import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import { getFileMeta } from "./utils.js";
import chalk from "chalk";
const { __dirname } = getFileMeta();
let projectSettings = null;
function checkExcludeFiles(destPath) {
  return (
    projectSettings?.excludeFilePath?.includes(destPath.replace(/\\/g, "/")) ??
    false
  );
}
/**
 * Recursively copies all files and folders from `src` to `dest`,
 * overwriting existing files if they already exist.
 *
 * The `base` parameter represents the original root of the copy.
 * It is used for computing a relative path for cleaner logging.
 */
function copyRecursiveSync(src, dest, base = src, isPrismaPHP = false) {
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
    const relative = path.relative(base, srcPath);
    const display = path.join(path.basename(base), relative);
    if (checkExcludeFiles(destPath)) return;
    // **Normalize Path to Avoid Issues on Windows**
    const relativePath = path.relative(__dirname, srcPath);
    const validatorFile = path.normalize("src/Lib/Validator.php");
    // **Skip Validator.php when isPrismaPHP is true**
    if (isPrismaPHP && relativePath === validatorFile) {
      console.log(`Skipping file: ${display}`);
      continue;
    }
    if (entry.isDirectory()) {
      copyRecursiveSync(srcPath, destPath, base, isPrismaPHP);
    } else {
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
function installNpmDependencies(isPrismaPHP) {
  const currentDir = process.cwd();
  const packageJsonPath = path.join(currentDir, "package.json");
  let packageJson;
  if (fs.existsSync(packageJsonPath)) {
    const packageJsonContent = fs.readFileSync(packageJsonPath, "utf8");
    packageJson = JSON.parse(packageJsonContent);
    packageJson.prisma = {
      seed: "tsx prisma/seed.ts",
    };
    if (!isPrismaPHP) {
      packageJson.type = "module";
    }
    fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2));
  } else {
    console.error("package.json does not exist.");
    return;
  }
  let npmDependencies = [
    npmPkg("prisma"),
    npmPkg("@prisma/client"),
    npmPkg("@prisma/internals"),
  ];
  if (!isPrismaPHP) {
    npmDependencies.push(
      npmPkg("tsx"),
      npmPkg("typescript"),
      npmPkg("@types/node")
    );
  }
  const packagesStr = npmDependencies.join(" ");
  try {
    console.log(`Installing packages: ${packagesStr}`);
    execSync(`npm install --save-dev ${packagesStr}`, {
      stdio: "inherit",
      cwd: currentDir,
    });
    if (!isPrismaPHP) {
      execSync("npx tsc --init", {
        stdio: "inherit",
        cwd: currentDir,
      });
    }
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
  const currentDir = process.cwd();
  const prismaFolderPath = path.join(currentDir, "prisma");
  if (fs.existsSync(prismaFolderPath)) {
    console.warn("Prisma folder already exists. Skipping prisma init.");
    return;
  }
  try {
    console.log("Initializing Prisma...");
    execSync(`npx prisma init`, {
      stdio: "inherit",
      cwd: currentDir,
    });
    console.log("Prisma initialized successfully.");
  } catch (error) {
    console.error("Error initializing Prisma:", error);
  }
}
async function installComposerDependencies(isPrismaPHP) {
  const currentDir = process.cwd();
  const composerJsonPath = path.join(currentDir, "composer.json");
  if (!fs.existsSync(composerJsonPath)) {
    console.error("composer.json does not exist.");
    return;
  }
  let composerDependencies = [composerPkg("calicastle/cuid")];
  if (isPrismaPHP) {
    const configPath = path.join(currentDir, "prisma-php.json");
    if (fs.existsSync(configPath)) {
      const localSettings = readJsonFile(configPath);
      let excludeFiles = [];
      localSettings.excludeFiles?.map((file) => {
        const filePath = path.join(currentDir, file);
        if (fs.existsSync(filePath))
          excludeFiles.push(filePath.replace(/\\/g, "/"));
      });
      projectSettings = {
        ...localSettings,
        excludeFiles: localSettings.excludeFiles ?? [],
        excludeFilePath: excludeFiles ?? [],
      };
    }
  } else {
    composerDependencies.push(
      composerPkg("ezyang/htmlpurifier"),
      composerPkg("symfony/uid"),
      composerPkg("brick/math")
    );
  }
  try {
    // Log the dependencies being installed
    console.log("Installing Composer dependencies:");
    composerDependencies.forEach((dep) => console.log(`- ${chalk.blue(dep)}`));
    // Prepare the composer require command
    const composerRequireCommand = `C:\\xampp\\php\\php.exe C:\\ProgramData\\ComposerSetup\\bin\\composer.phar require ${composerDependencies.join(
      " "
    )}`;
    // Execute the composer require command
    execSync(composerRequireCommand, {
      stdio: "inherit",
      cwd: currentDir,
    });
  } catch (error) {
    console.error("Error installing Composer dependencies:", error);
  }
}
const readJsonFile = (filePath) => {
  const jsonData = fs.readFileSync(filePath, "utf8");
  return JSON.parse(jsonData);
};
const npmPinnedVersions = {
  prisma: "^6.5.0",
  "@prisma/client": "^6.5.0",
  "@prisma/internals": "^6.5.0",
  tsx: "^4.19.3",
  typescript: "^5.8.2",
  "@types/node": "^22.13.11",
};
function npmPkg(name) {
  return npmPinnedVersions[name] ? `${name}@${npmPinnedVersions[name]}` : name;
}
const composerPinnedVersions = {
  "ezyang/htmlpurifier": "^4.18.0",
  "calicastle/cuid": "^2.0.0",
  "symfony/uid": "^7.2.0",
  "brick/math": "^0.13.0",
};
function composerPkg(name) {
  return composerPinnedVersions[name]
    ? `${name}:${composerPinnedVersions[name]}`
    : name;
}
async function main() {
  const isPrismaPHP = process.argv.includes("--prisma-php");
  installNpmDependencies(isPrismaPHP);
  installComposerDependencies(isPrismaPHP);
  initPrisma();
  directoriesToCopy.forEach((config) => {
    copyRecursiveSync(
      config.srcFolder,
      config.destFolder,
      config.srcFolder,
      isPrismaPHP
    );
  });
  console.log("Finished copying directories.");
}
// Run the main function
main().catch((error) => {
  console.error("Error during execution:", error);
});
