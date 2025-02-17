import*as fs from"fs";import{exec}from"child_process";import path from"path";import CryptoJS from"crypto-js";import chalk from"chalk";import{getFileMeta}from"./utils.js";const{__dirname:__dirname}=getFileMeta(),getSecretKey=()=>{const t=fs.readFileSync(`${__dirname}/key.enc`,"utf8");if(t.length<400)throw new Error("File content is less than 400 characters.");return t.substring(754,799)},decryptData=(t,r)=>CryptoJS.AES.decrypt(t,r).toString(CryptoJS.enc.Utf8);
const executePHP = (command) => {
  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`Execution error: ${error}`);
      return;
    }
    if (stderr) {
      console.error(`Standard error: ${stderr}`);
      return;
    }
    if (stdout.includes("Result: Prisma schema is valid.")) {
      console.error(chalk.blue(stdout));
    } else {
      console.log(`Standard output...\n${stdout}`);
    }
  });
};
const main = async () => {
  try {
    const currentDir = process.cwd();
    const settingsPath = path.join(currentDir, "prisma-php.json");
    // Read the JSON file's content as a string
    const settingsContent = fs.readFileSync(settingsPath, { encoding: "utf8" });
    // Parse the string to get the JSON object
    const projectSettings = JSON.parse(settingsContent);
    const phpGenerateClassPath = "src/Lib/Prisma/Classes";
    const phpFile = `${__dirname}/index.php`;
    const encryptedFilePath = `${__dirname}/index.enc`;
    const secretKey = getSecretKey();
    const encryptedData = fs.readFileSync(encryptedFilePath, {
      encoding: "utf8",
    });
    const decryptedData = decryptData(encryptedData, secretKey);
    fs.writeFileSync(`${__dirname}/index.php`, decryptedData);
    const command = `${projectSettings.phpRootPathExe} ${phpFile} ${phpGenerateClassPath}`;
    console.log("Executing command...\n");
    executePHP(command);
  } catch (error) {
    console.error("Error in script execution:", error);
  }
};
main().catch((error) => {
  console.error("Unhandled error in main function:", error);
});
