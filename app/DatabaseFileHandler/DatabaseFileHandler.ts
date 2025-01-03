import { FileHandle, open } from "fs/promises";
import { constants } from "fs";

export class DatabaseFileHandler {
  private fileName: string;
  private fileHandle: FileHandle | null;

  private constructor(fileName: string) {
    this.fileName = fileName;
    this.fileHandle = null;
  }

  static async create(fileName: string): Promise<FileHandle | null> {
    const reader = new DatabaseFileHandler(fileName);
    await reader.readFile();
    return reader.fileHandle;
  }

  private async readFile() {
    this.fileHandle = await open(this.fileName, constants.O_RDONLY);
  }
}
