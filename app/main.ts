#!/usr/bin/env node

import { Command } from "commander";
import { description, version } from "../package.json";
import { DatabaseFileHandler } from "./DatabaseFileHandler/DatabaseFileHandler";
import { DatabaseFileParser } from "./DatabaseFileParser/DatabaseFileParser";
const program = new Command();

program
  .version(version)
  .description(description)
  .requiredOption("-f, --file <file>", "db file to read")
  .requiredOption("-c, --command <command>", "command to run against db")
  .parse(process.argv);

const { file, command } = program.opts();

async function main() {
  if (file && command) {
    const databaseFileHandle = await DatabaseFileHandler.create(file);

    if (databaseFileHandle) {
      const databaseFileParser = new DatabaseFileParser(databaseFileHandle);
      const { pageSize, pageCount, numberOfTables, tableNames } = await databaseFileParser.parse();

      // TODO: add query commands
      switch (command) {
        case ".dbinfo":
          console.log(`Page size: ${pageSize}`);
          console.log(`Page count: ${pageCount}`);
          console.log(`Number of tables: ${numberOfTables}`);
          console.log(`Table names: ${tableNames.join(", ")}`);
          break;
      }

      await databaseFileHandle.close();
    }
  }
}

main().catch(console.error);
