export const readVarint = (buffer: Uint8Array): { value: number; nextOffset: number } => {
  let result = 0;
  let index = 0;
  let shift = 0;

  while (index < buffer.length && index < 8) {
    const byte = buffer[index];
    result |= (byte & 0x7f) << shift; // Accumulate the 7 bits at the correct position

    if (byte < 0x80) {
      // Check if MSB is 0, indicating end of varint
      break;
    }
    shift += 7;
    index++;
  }

  return { value: result, nextOffset: index + 1 };
};

export const parseSerialTypeValue = (buffer: Buffer, targetSerialType: number, id: number): string => {
  // Parse value based on serial type
  if (targetSerialType >= 13 && targetSerialType % 2 === 1) {
    // Text
    return buffer.toString("utf-8");
  } else if (targetSerialType >= 12 && targetSerialType % 2 === 0) {
    // BLOB
    return buffer.toString("hex");
  } else if (targetSerialType === 1) {
    return buffer.readInt8(0).toString();
  } else if (targetSerialType === 2) {
    return buffer.readInt16BE(0).toString();
  } else if (targetSerialType === 3) {
    return buffer.readIntBE(0, 3).toString();
  } else if (targetSerialType === 4) {
    return buffer.readInt32BE(0).toString();
  } else if (targetSerialType === 5) {
    return buffer.readIntBE(0, 6).toString();
  } else if (targetSerialType === 6) {
    return buffer.readBigInt64BE(0).toString();
  } else if (targetSerialType === 7) {
    return buffer.readDoubleBE(0).toString();
  } else if (targetSerialType === 8 || targetSerialType === 9) {
    return "0";
  } else {
    return `${id}`;
  }
};

export const getSerialTypeSize = (serialType: number): number => {
  if (serialType === 0) return 0; // NULL
  if (serialType === 1) return 1; // 8-bit integer
  if (serialType === 2) return 2; // 16-bit integer
  if (serialType === 3) return 3; // 24-bit integer
  if (serialType === 4) return 4; // 32-bit integer
  if (serialType === 5) return 6; // 48-bit integer
  if (serialType === 6) return 8; // 64-bit integer
  if (serialType === 7) return 8; // 64-bit float
  if (serialType === 8 || serialType === 9) return 0; // Reserved integers 0 or 1
  if (serialType >= 12 && serialType % 2 === 0) return (serialType - 12) / 2; // BLOB
  if (serialType >= 13 && serialType % 2 === 1) return (serialType - 13) / 2; // Text
  return 0;
};

export const parseCellPayload = (buffer: Buffer, offset: number, rowId: number = -99999): string[] => {
  const { value: headerSize, nextOffset: headerSizeBytes } = readVarint(buffer.subarray(offset));
  offset += headerSizeBytes;

  const serialTypes: number[] = [];
  let headerRemainingBytes = headerSize - headerSizeBytes;

  while (headerRemainingBytes > 0) {
    const { value: serialType, nextOffset: serialTypeBytes } = readVarint(buffer.subarray(offset));
    serialTypes.push(serialType);
    offset += serialTypeBytes;
    headerRemainingBytes -= serialTypeBytes;
  }

  const data = [];

  for (let i = 0; i < serialTypes.length; i++) {
    const columnSize = getSerialTypeSize(serialTypes[i]);
    data.push(parseSerialTypeValue(buffer.subarray(offset, offset + columnSize), serialTypes[i], rowId));
    offset += columnSize;
  }

  return data;
};

export const parseTableLeafCell = (buffer: Buffer) => {
  let offset = 0;

  const { nextOffset: payloadSizeBytes } = readVarint(buffer.subarray(offset));
  offset += payloadSizeBytes;

  const { value: rowId, nextOffset: rowIdBytes } = readVarint(buffer.subarray(offset));
  offset += rowIdBytes;

  return parseCellPayload(buffer, offset, rowId);
};

export const parseIndexLeafCell = (buffer: Buffer) => {
  let offset = 0;
  const { nextOffset: payloadSizeBytes } = readVarint(buffer.subarray(offset));
  offset += payloadSizeBytes;

  const [indexedValue, id] = parseCellPayload(buffer, offset);
  return { indexedValue, id };
};
