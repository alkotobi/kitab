export function readMagic(view, offset) {
  return String.fromCharCode(
    view.getUint8(offset + 0),
    view.getUint8(offset + 1),
    view.getUint8(offset + 2),
    view.getUint8(offset + 3)
  );
}

export function readU64(view, offset) {
  const lo = view.getUint32(offset + 0, true);
  const hi = view.getUint32(offset + 4, true);
  return BigInt(lo) | (BigInt(hi) << 32n);
}

export function readBooksHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const compression = view.getUint32(8, true);
  const blockSize = view.getUint32(12, true);
  const blockCount = readU64(view, 20);
  const indexOffset = readU64(view, 28);
  return { magic, version, compression, blockSize, blockCount, indexOffset };
}

export function readBlockIndexEntry(view, baseOffsetBytes, index) {
  const size = 32;
  const off = baseOffsetBytes + index * size;
  const uncompressedOffset = readU64(view, off + 0);
  const uncompressedSize = view.getUint32(off + 8, true);
  const compressedOffset = readU64(view, off + 12);
  const compressedSize = view.getUint32(off + 20, true);
  return { uncompressedOffset, uncompressedSize, compressedOffset, compressedSize };
}

export function readBooksIndexHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const bookCount = view.getUint32(8, true);
  return { magic, version, bookCount };
}

export function readBookIndexEntry(view, baseOffsetBytes, index) {
  const size = 40;
  const off = baseOffsetBytes + index * size;
  const book_id = view.getUint32(off + 0, true);
  const first_chapter_id = view.getUint32(off + 4, true);
  const chapter_count = view.getUint32(off + 8, true);
  const first_page_id = view.getUint32(off + 12, true);
  const page_count = view.getUint32(off + 16, true);
  const title_index = view.getUint32(off + 20, true);
  const text_start_offset = readU64(view, off + 24);
  const text_end_offset = readU64(view, off + 32);
  return {
    book_id,
    first_chapter_id,
    chapter_count,
    first_page_id,
    page_count,
    title_index,
    text_start_offset,
    text_end_offset
  };
}

export function readPagesIndexHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const pageCount = view.getUint32(8, true);
  return { magic, version, pageCount };
}

export function readPageIndexEntry(view, baseOffsetBytes, index) {
  const size = 32;
  const off = baseOffsetBytes + index * size;
  const page_id = view.getUint32(off + 0, true);
  const book_id = view.getUint32(off + 4, true);
  const chapter_id = view.getUint32(off + 8, true);
  const page_number = view.getUint32(off + 12, true);
  const block_id = view.getUint32(off + 16, true);
  const offset_in_block = view.getUint32(off + 24, true);
  const length = view.getUint32(off + 28, true);
  return {
    page_id,
    book_id,
    chapter_id,
    page_number,
    block_id,
    offset_in_block,
    length
  };
}

export function readChaptersIndexHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const chapterCount = view.getUint32(8, true);
  return { magic, version, chapterCount };
}

export function readChapterIndexEntry(view, baseOffsetBytes, index) {
  const size = 32;
  const off = baseOffsetBytes + index * size;
  const chapter_id = view.getUint32(off + 0, true);
  const book_id = view.getUint32(off + 4, true);
  const chapter_number = view.getUint32(off + 8, true);
  const first_page_id = view.getUint32(off + 12, true);
  const page_count = view.getUint32(off + 16, true);
  const title_index = view.getUint32(off + 20, true);
  return {
    chapter_id,
    book_id,
    chapter_number,
    first_page_id,
    page_count,
    title_index
  };
}

export function readTitlesHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const titleCount = view.getUint32(8, true);
  const stringsOffset = readU64(view, 16);
  return { magic, version, titleCount, stringsOffset };
}

export function readTitleEntry(view, baseOffsetBytes, index) {
  const size = 16;
  const off = baseOffsetBytes + index * size;
  const offset = readU64(view, off + 0);
  const length = view.getUint32(off + 8, true);
  const flags = view.getUint32(off + 12, true);
  return { offset, length, flags };
}

export function readTitleString(view, header, entry) {
  const decoder = new TextDecoder("utf-8");
  const start = Number(header.stringsOffset + entry.offset);
  const bytes = new Uint8Array(view.buffer, view.byteOffset + start, entry.length);
  return decoder.decode(bytes);
}

export function readWordsIndexHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const word_count = view.getUint32(8, true);
  const words_bin_size = readU64(view, 16);
  const postings_file_size = readU64(view, 24);
  return { magic, version, word_count, words_bin_size, postings_file_size };
}

export function readWordIndexEntry(view, baseOffsetBytes, index) {
  const size = 40;
  const off = baseOffsetBytes + index * size;
  const word_id = view.getUint32(off + 0, true);
  const df = view.getUint32(off + 4, true);
  const cf = view.getUint32(off + 8, true);
  const postings_count = view.getUint32(off + 12, true);
  const postings_block_id = view.getUint32(off + 16, true);
  const postings_offset_in_block = view.getUint32(off + 20, true);
  const postings_length_in_block = view.getUint32(off + 24, true);
  const word_string_offset = view.getUint32(off + 28, true);
  const word_string_length = view.getUint32(off + 32, true);
  const flags = view.getUint32(off + 36, true);
  return {
    word_id,
    df,
    cf,
    postings_count,
    postings_block_id,
    postings_offset_in_block,
    postings_length_in_block,
    word_string_offset,
    word_string_length,
    flags
  };
}

export function readPostingsHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const flags = view.getUint32(8, true);
  const total_postings = readU64(view, 16);
  const block_count = readU64(view, 24);
  const block_index_offset = readU64(view, 32);
  const blocks_data_offset = readU64(view, 40);
  return {
    magic,
    version,
    flags,
    total_postings,
    block_count,
    block_index_offset,
    blocks_data_offset
  };
}

export function readPostingsBlockIndexEntry(view, baseOffsetBytes, index) {
  const size = 40;
  const off = baseOffsetBytes + index * size;
  const first_word_id = readU64(view, off + 0);
  const last_word_id = readU64(view, off + 8);
  const uncompressed_size = readU64(view, off + 16);
  const compressed_offset = readU64(view, off + 24);
  const compressed_size = readU64(view, off + 32);
  return {
    first_word_id,
    last_word_id,
    uncompressed_size,
    compressed_offset,
    compressed_size
  };
}
