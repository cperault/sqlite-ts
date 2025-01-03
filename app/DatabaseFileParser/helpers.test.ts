import { readVarint, parseSerialTypeValue, getSerialTypeSize, parseCellPayload, parseTableLeafCell, parseIndexLeafCell } from "./helpers";

describe("helpers.ts", () => {
  describe("readVarint", () => {
    it("should read a single byte varint", () => {
      const buffer = new Uint8Array([0x7f]);
      const result = readVarint(buffer);
      expect(result).toEqual({ value: 127, nextOffset: 1 });
    });

    it("should read a multi-byte varint", () => {
      const buffer = new Uint8Array([0x81, 0x01]);
      const result = readVarint(buffer);
      expect(result).toEqual({ value: 129, nextOffset: 2 });
    });

    it("should read a maximum length varint", () => {
      const buffer = new Uint8Array([0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]);
      const result = readVarint(buffer);
      expect(result).toEqual({ value: (1 << 49) + 1, nextOffset: 8 });
    });

    it("should handle varint with continuation bits", () => {
      const buffer = new Uint8Array([0x81, 0x80, 0x01]);
      const result = readVarint(buffer);
      expect(result).toEqual({ value: 16385, nextOffset: 3 });
    });
  });

  describe("parseSerialTypeValue", () => {
    it("should parse text serial type", () => {
      const buffer = Buffer.from("hello", "utf-8");
      const result = parseSerialTypeValue(buffer, 13, 1);
      expect(result).toBe("hello");
    });

    it("should parse BLOB serial type", () => {
      const buffer = Buffer.from("hello", "utf-8");
      const result = parseSerialTypeValue(buffer, 14, 1);
      expect(result).toBe(buffer.toString("hex"));
    });

    it("should parse fixed-length serial types", () => {
      const buffer = Buffer.from([0x01]);
      const result = parseSerialTypeValue(buffer, 1, 1);
      expect(result).toBe("1");
    });
  });

  describe("getSerialTypeSize", () => {
    it("should return size for predefined serial types", () => {
      expect(getSerialTypeSize(1)).toBe(1);
      expect(getSerialTypeSize(2)).toBe(2);
      expect(getSerialTypeSize(3)).toBe(3);
      expect(getSerialTypeSize(4)).toBe(4);
      expect(getSerialTypeSize(5)).toBe(6);
      expect(getSerialTypeSize(6)).toBe(8);
      expect(getSerialTypeSize(7)).toBe(8);
      expect(getSerialTypeSize(8)).toBe(0);
      expect(getSerialTypeSize(9)).toBe(0);
    });

    it("should return size for BLOB and Text", () => {
      expect(getSerialTypeSize(12)).toBe(0);
      expect(getSerialTypeSize(13)).toBe(0);
      expect(getSerialTypeSize(14)).toBe(1);
      expect(getSerialTypeSize(15)).toBe(1);
      expect(getSerialTypeSize(100)).toBe(44);
    });
  });

  describe("parseCellPayload", () => {
    it("should parse cell payload", () => {
      const buffer = Buffer.from([0x02, 0x01, 0x01, 0x01]);
      const result = parseCellPayload(buffer, 0);
      expect(result).toEqual(["1"]);
    });
  });

  // TODO: fix
  describe.skip("parseTableLeafCell", () => {
    it("should parse table leaf cell", () => {
      const buffer = Buffer.from([
        0x04, // Payload size
        0x01, // Row ID
        0x0d, // Serial type (text, '1')
        0x31, // ASCII '1'
      ]);
      const result = parseTableLeafCell(buffer);

      expect(result).toEqual(["1"]); // ASCII '1'
    });
  });

  // TODO: fix
  describe.skip("parseIndexLeafCell", () => {
    it("should parse index leaf cell", () => {
      const buffer = Buffer.from([
        0x06, // Payload size
        0x03, // Row ID
        0x0d, // Serial type (text, '1') for indexedValue
        0x31, // ASCII '1'
        0x0d, // Serial type (text, '1') for ID
        0x31, // ASCII '1'
      ]);
      const result = parseIndexLeafCell(buffer);

      expect(result).toEqual({ indexedValue: "1", id: "1" });
    });
  });
});
