import { fileURLToPath } from "url";
import { dirname } from "path";
/**
 * Retrieves the file metadata including the filename and directory name.
 *
 * @param importMetaUrl - The URL of the module's import.meta.url.
 * @returns An object containing the filename (`__filename`) and directory name (`__dirname`).
 */
export function getFileMeta() {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    return { __filename, __dirname };
}
