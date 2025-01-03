import { FileHandle } from "fs/promises";
import { parseTableLeafCell } from "./helpers";

export interface DatabaseInfo {
  pageSize: number;
  pageCount: number;
  numberOfTables: number;
  tableNames: string[];
}

// https://www.sqlite.org/fileformat.html 1.3 The Database Header
export interface DatabaseFileHeader {
  "header string": string; // The header string: "SQLite format 3\000".
  "database page size": number; // The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
  "file format write version": number; // File format write version. 1 for legacy; 2 for WAL.
  "file format read version": number; // File format read version. 1 for legacy; 2 for WAL.
  "reserved space at end of each page": number; // Bytes of unused "reserved" space at the end of each page. Usually 0.
  "maximum embedded payload fraction": number; // Maximum embedded payload fraction. Must be 64.
  "minimum embedded payload fraction": number; // Minimum embedded payload fraction. Must be 32.
  "leaf payload fraction": number; // Leaf payload fraction. Must be 32.
  "file change counter": number; // File change counter.
  "number of pages": number; // Size of the database file in pages. The "in-header database size".
  "first freeList trunk page": number; // Page number of the first freeList trunk page.
  "total freeList pages": number; // Total number of freeList pages.
  "schema cookie": number; // The schema cookie.
  "schema format number": number; // Supported schema formats are 1, 2, 3, and 4.
  "default page cache size": number; // Default page cache size.
  "largest root b-tree page number": number; // The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
  "database text encoding": number; // The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
  "user version": number; // The "user version" set by the user_version pragma.
  "incremental vacuum mode": number; // True (non-zero) for incremental-vacuum mode, false otherwise.
  "application ID": number; // The "Application ID" set by PRAGMA application_id.
  "reserved for expansion": Uint8Array; // Reserved for expansion. Must be zero.
  "version valid for number": number; // The version-valid-for number.
  "sqlite version number": number; // SQLITE_VERSION_NUMBER.
}

// https://www.sqlite.org/fileformat.html 1.6. B-tree Pages
export interface BTreePageType {
  "interior index b-tree page": number;
  "interior table b-tree page": number;
  "leaf index b-tree page": number;
  "leaf table b-tree page": number;
}

const B_TREE_PAGE_TYPE: BTreePageType = {
  "interior index b-tree page": 0x02, // 2
  "interior table b-tree page": 0x05, // 5
  "leaf index b-tree page": 0x0a, // 10
  "leaf table b-tree page": 0x0d, // 13
};

export interface BTreePageHeader {
  "number of tables": number; // The two-byte integer at offset 3 gives the number of tables on the page.
  "b-tree page type": number; // The one-byte flag at offset 0 indicating the b-tree page type.
  "start of first freeBlock": number; // The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
  "number of cells": number; // The two-byte integer at offset 3 gives the number of cells on the page.
  "start of cell content area": number; // The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
  "number of fragmented free bytes": number; // The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
  "right-most pointer": number | null; // The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
}

export interface BTreePageHeaderInfo extends BTreePageHeader {
  headerSize: number;
}

export enum PageCellData {
  schemaType,
  schemaName,
  schemaTableName,
  schemaRootPage,
  schema,
}

const maxByteSize = 100; // we don't want buffer or cell pointers to be larger than this

export class DatabaseFileParser {
  private databaseFileHeader: DatabaseFileHeader;
  private databasePageHeader: BTreePageHeader;
  private fileHandle: FileHandle;
  private cellPointersArray: number[];
  private pageBuffer: Buffer;

  constructor(fileHandle: FileHandle) {
    this.databaseFileHeader = {} as DatabaseFileHeader;
    this.databasePageHeader = {} as BTreePageHeader;
    this.fileHandle = fileHandle;
    this.cellPointersArray = [];
    this.pageBuffer = Buffer.alloc(maxByteSize);
  }

  public async parse(): Promise<DatabaseInfo> {
    this.databaseFileHeader = await this.parseDatabaseFileHeader();
    this.databasePageHeader = await this.parsePageHeader(maxByteSize);
    this.cellPointersArray = await this.getCellPointerArray(maxByteSize);
    this.pageBuffer = Buffer.from(await this.getBufferFromOffset(this.databaseFileHeader["database page size"], 0));

    return {
      pageSize: this.getPageSize(),
      pageCount: this.getPageCount(),
      numberOfTables: await this.getNumberOfTables(),
      tableNames: this.getTableNames(),
    };
  }

  private async getBufferFromOffset(bufferSize: number, offset: number) {
    const buffer = new Uint8Array(bufferSize);
    await this.fileHandle.read(buffer, 0, buffer.length, offset);
    return buffer;
  }

  private async parseDatabaseFileHeader(): Promise<DatabaseFileHeader> {
    const databaseFileHeaderBuffer = await this.getBufferFromOffset(maxByteSize, 0);
    const databaseFileHeaderData = new DataView(databaseFileHeaderBuffer.buffer, 0, databaseFileHeaderBuffer.length);

    return {
      "header string": String.fromCharCode(...new Uint8Array(databaseFileHeaderBuffer.slice(0, 16))),
      "database page size": databaseFileHeaderData.getUint16(16),
      "file format write version": databaseFileHeaderData.getUint8(18),
      "file format read version": databaseFileHeaderData.getUint8(19),
      "reserved space at end of each page": databaseFileHeaderData.getUint8(20),
      "maximum embedded payload fraction": databaseFileHeaderData.getUint8(21),
      "minimum embedded payload fraction": databaseFileHeaderData.getUint8(22),
      "leaf payload fraction": databaseFileHeaderData.getUint8(23),
      "file change counter": databaseFileHeaderData.getUint32(24),
      "number of pages": databaseFileHeaderData.getUint32(28),
      "first freeList trunk page": databaseFileHeaderData.getUint32(32),
      "total freeList pages": databaseFileHeaderData.getUint32(36),
      "schema cookie": databaseFileHeaderData.getUint32(40),
      "schema format number": databaseFileHeaderData.getUint32(44),
      "default page cache size": databaseFileHeaderData.getUint32(48),
      "largest root b-tree page number": databaseFileHeaderData.getUint32(52),
      "database text encoding": databaseFileHeaderData.getUint32(56),
      "user version": databaseFileHeaderData.getUint32(60),
      "incremental vacuum mode": databaseFileHeaderData.getUint32(64),
      "application ID": databaseFileHeaderData.getUint32(68),
      "reserved for expansion": new Uint8Array(databaseFileHeaderBuffer.slice(72, 92)),
      "version valid for number": databaseFileHeaderData.getUint32(92),
      "sqlite version number": databaseFileHeaderData.getUint32(96),
    };
  }

  private async parsePageHeader(offset: number): Promise<BTreePageHeaderInfo> {
    const pageHeaderBuffer = await this.getBufferFromOffset(12, offset);
    const pageHeaderData = new DataView(pageHeaderBuffer.buffer, 0, pageHeaderBuffer.length);

    const pagerHeaderSize =
      pageHeaderData.getUint8(0) === B_TREE_PAGE_TYPE["leaf table b-tree page"] || pageHeaderData.getInt8(8) === B_TREE_PAGE_TYPE["leaf index b-tree page"]
        ? 8
        : 12;

    return {
      "number of tables": pageHeaderData.getUint16(3),
      "b-tree page type": pageHeaderData.getUint8(0),
      "start of first freeBlock": pageHeaderData.getUint16(1),
      "number of cells": pageHeaderData.getUint16(3),
      "start of cell content area": pageHeaderData.getUint16(5),
      "number of fragmented free bytes": pageHeaderData.getUint8(7),
      "right-most pointer": pageHeaderData.getUint32(8) - 1,
      headerSize: pagerHeaderSize,
    };
  }

  private async getCellPointerArray(offset: number) {
    const { "number of cells": cellCount, headerSize } = await this.parsePageHeader(offset);
    const pointerArray: number[] = [];
    const pointerBuffer = await this.getBufferFromOffset(cellCount * 2, offset + headerSize);

    for (let c = 0; c < cellCount; c++) {
      const cellOffset = c * 2;

      if (c * 2 + 2 <= pointerBuffer.length) {
        pointerArray.push(new DataView(pointerBuffer.buffer, 0, pointerBuffer.length).getUint16(cellOffset));
      } else {
        throw new RangeError(`Offset ${cellOffset} is outside the bounds of the DataView`);
      }
    }

    return pointerArray;
  }

  private getPageSize(): number {
    const databasePageSize: number = this.databaseFileHeader["database page size"];
    return databasePageSize;
  }

  private getPageCount(): number {
    const databasePageCount: number = this.databaseFileHeader["number of pages"];
    return databasePageCount;
  }

  private async getNumberOfTables(): Promise<number> {
    const databaseTableCount: number = this.databasePageHeader["number of tables"];
    return databaseTableCount;

    /* The other, more manual implementation I came up with before I realized
       table count was already provided from page header data --facepalm--

    let tableCount = 0;

    for (let page = 1; page <= this.getPageCount(); page++) {
      const pageData = new Uint8Array(1);
      await this.fileHandle.read(pageData, 0, pageData.length, page * this.getPageSize());

      if (pageData[0] === 0x0d) {
        // we've found a leaf table b-tree page
        tableCount++;
      }
    }

    return tableCount;
    */
  }

  private getTableNames(): string[] {
    let tableNames: string[] = [];

    this.cellPointersArray.forEach((cellPointer) => tableNames.push(parseTableLeafCell(this.pageBuffer.subarray(cellPointer))[PageCellData.schemaTableName]));

    return tableNames;
  }
}
