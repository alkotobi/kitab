const OFFLINE_DB_NAME = "jamharah-offline";
const OFFLINE_DB_VERSION = 1;
const OFFLINE_STORE = "files";

let offlineDbPromise = null;

const jhTextEncoder = new TextEncoder();
const jhTextDecoder = new TextDecoder("utf-8");
const jhHashCache = new Map();
const JH_HASH_CACHE_LIMIT = 1024;

function openOfflineDb() {
  if (!("indexedDB" in globalThis)) {
    return Promise.resolve(null);
  }
  if (offlineDbPromise) {
    return offlineDbPromise;
  }
  offlineDbPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(OFFLINE_DB_NAME, OFFLINE_DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(OFFLINE_STORE)) {
        db.createObjectStore(OFFLINE_STORE, { keyPath: "name" });
      }
    };
    req.onsuccess = () => {
      resolve(req.result);
    };
    req.onerror = () => {
      reject(req.error);
    };
  });
  return offlineDbPromise;
}

async function getOfflineFileBuffer(name) {
  const db = await openOfflineDb();
  if (!db) {
    return null;
  }
  return new Promise((resolve, reject) => {
    const tx = db.transaction(OFFLINE_STORE, "readonly");
    const store = tx.objectStore(OFFLINE_STORE);
    const req = store.get(name);
    req.onsuccess = () => {
      const rec = req.result;
      resolve(rec ? rec.buffer : null);
    };
    req.onerror = () => {
      reject(req.error);
    };
  });
}

async function putOfflineFileBuffer(name, buffer) {
  const db = await openOfflineDb();
  if (!db) {
    return;
  }
  return new Promise((resolve, reject) => {
    const tx = db.transaction(OFFLINE_STORE, "readwrite");
    const store = tx.objectStore(OFFLINE_STORE);
    store.put({ name, buffer });
    tx.oncomplete = () => {
      resolve();
    };
    tx.onerror = () => {
      reject(tx.error);
    };
  });
}

export async function getOfflineFileInfo(name) {
  const db = await openOfflineDb();
  if (!db) {
    return { name, offline: false, size: null };
  }
  return new Promise((resolve, reject) => {
    const tx = db.transaction(OFFLINE_STORE, "readonly");
    const store = tx.objectStore(OFFLINE_STORE);
    const req = store.get(name);
    req.onsuccess = () => {
      const rec = req.result;
      if (!rec) {
        resolve({ name, offline: false, size: null });
      } else {
        const size =
          rec.buffer && typeof rec.buffer.byteLength === "number"
            ? rec.buffer.byteLength
            : null;
        resolve({ name, offline: true, size });
      }
    };
    req.onerror = () => {
      reject(req.error);
    };
  });
}

export async function getOfflineFilesInfo(names) {
  const out = [];
  for (let i = 0; i < names.length; i++) {
    out.push(await getOfflineFileInfo(names[i]));
  }
  return out;
}

const dataViewCache = new Map();
const postingsPagesCache = new Map();
const postingsListCache = new Map();
const pageTextCache = new Map();

export function clearBinaryCache() {
  dataViewCache.clear();
}

export function clearPostingsPagesCache() {
  postingsPagesCache.clear();
}

export function clearPostingsListCache() {
  postingsListCache.clear();
}

export function clearPageTextCache() {
  pageTextCache.clear();
}

export function clearAllCaches() {
  clearBinaryCache();
  clearPostingsPagesCache();
  clearPostingsListCache();
  clearPageTextCache();
}

export async function loadBinaryAsDataView(url) {
  if (dataViewCache.has(url)) {
    return dataViewCache.get(url);
  }
  const promise = (async () => {
    const offlineBuf = await getOfflineFileBuffer(url);
    if (offlineBuf) {
      return new DataView(offlineBuf);
    }
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error("failed to fetch " + url + " status=" + res.status);
    }
    const buf = await res.arrayBuffer();
    return new DataView(buf);
  })();
  dataViewCache.set(url, promise);
  return promise;
}

export async function downloadFileForOffline(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error("failed to download " + url + " status=" + res.status);
  }
  const buf = await res.arrayBuffer();
  await putOfflineFileBuffer(url, buf);
}

export async function downloadFilesForOffline(urls) {
  for (let i = 0; i < urls.length; i++) {
    await downloadFileForOffline(urls[i]);
  }
}

export async function fetchRangeAsDataView(url, start, length) {
  const s = BigInt(start);
  const l = BigInt(length);
  const e = s + l - 1n;
  const res = await fetch(url, {
    headers: {
      Range: "bytes=" + s.toString() + "-" + e.toString()
    }
  });
  if (!res.ok) {
    throw new Error("failed to fetch range " + url + " status=" + res.status);
  }
  const buf = await res.arrayBuffer();
  return new DataView(buf);
}

export async function fetchRangeAsUint8Array(url, start, length) {
  const view = await fetchRangeAsDataView(url, start, length);
  return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
}

export class BinaryReader {
  constructor(view, offset = 0) {
    this.view = view;
    this.offset = offset;
  }

  seek(offset) {
    this.offset = offset;
  }

  skip(bytes) {
    this.offset += bytes;
  }

  readU8() {
    const v = this.view.getUint8(this.offset);
    this.offset += 1;
    return v;
  }

  readU32() {
    const v = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return v;
  }

  readU64() {
    const lo = this.view.getUint32(this.offset + 0, true);
    const hi = this.view.getUint32(this.offset + 4, true);
    this.offset += 8;
    return BigInt(lo) | (BigInt(hi) << 32n);
  }

  readBytes(length) {
    const bytes = new Uint8Array(
      this.view.buffer,
      this.view.byteOffset + this.offset,
      length
    );
    this.offset += length;
    return bytes;
  }

  readUtf8String(length) {
    const bytes = this.readBytes(length);
    return jhTextDecoder.decode(bytes);
  }
}

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
  const start = Number(header.stringsOffset + entry.offset);
  const bytes = new Uint8Array(view.buffer, view.byteOffset + start, entry.length);
  return jhTextDecoder.decode(bytes);
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

export function readAnnoHeader(view) {
  const magic = String.fromCharCode(
    view.getUint8(0),
    view.getUint8(1),
    view.getUint8(2),
    view.getUint8(3)
  );
  const version = view.getUint32(4, true);
  const corpus_version = readU64(view, 8);
  const comments_count = readU64(view, 16);
  const formatting_count = readU64(view, 24);
  const highlights_count = readU64(view, 32);
  const comments_offset = readU64(view, 40);
  const formatting_offset = readU64(view, 48);
  const highlights_offset = readU64(view, 56);
  return {
    magic,
    version,
    corpus_version,
    comments_count,
    formatting_count,
    highlights_count,
    comments_offset,
    formatting_offset,
    highlights_offset
  };
}

export function readAnnoCommentEntry(view, baseOffsetBytes, index) {
  const size = 56;
  const off = baseOffsetBytes + index * size;
  const page_id = view.getUint32(off + 0, true);
  const anchor_start = view.getUint32(off + 4, true);
  const anchor_end = view.getUint32(off + 8, true);
  const flags = view.getUint16(off + 12, true);
  const kind = view.getUint16(off + 14, true);
  const author_id = view.getUint32(off + 16, true);
  const created_at_unix = readU64(view, off + 20);
  const updated_at_unix = readU64(view, off + 28);
  const text_offset = readU64(view, off + 36);
  const text_length = view.getUint32(off + 44, true);
  return {
    page_id,
    anchor_start,
    anchor_end,
    flags,
    kind,
    author_id,
    created_at_unix,
    updated_at_unix,
    text_offset,
    text_length
  };
}

export function readAnnoCommentText(view, textOffset, textLength) {
  const start = Number(textOffset);
  const bytes = new Uint8Array(
    view.buffer,
    view.byteOffset + start,
    textLength
  );
  return jhTextDecoder.decode(bytes);
}

export function readAnnoFormattingEntry(view, baseOffsetBytes, index) {
  const size = 24;
  const off = baseOffsetBytes + index * size;
  const page_id = view.getUint32(off + 0, true);
  const anchor_start = view.getUint32(off + 4, true);
  const anchor_end = view.getUint32(off + 8, true);
  const flags = view.getUint16(off + 12, true);
  const style_id = view.getUint16(off + 14, true);
  const layer = view.getUint16(off + 16, true);
  const priority = view.getUint16(off + 18, true);
  return {
    page_id,
    anchor_start,
    anchor_end,
    flags,
    style_id,
    layer,
    priority
  };
}

export function readAnnoHighlightEntry(view, baseOffsetBytes, index) {
  const size = 48;
  const off = baseOffsetBytes + index * size;
  const page_id = view.getUint32(off + 0, true);
  const anchor_start = view.getUint32(off + 4, true);
  const anchor_end = view.getUint32(off + 8, true);
  const flags = view.getUint16(off + 12, true);
  const color_id = view.getUint16(off + 14, true);
  const category_id = view.getUint16(off + 16, true);
  const author_id = view.getUint32(off + 20, true);
  const created_at_unix = readU64(view, off + 24);
  const updated_at_unix = readU64(view, off + 32);
  const comment_ref_offset = readU64(view, off + 40);
  return {
    page_id,
    anchor_start,
    anchor_end,
    flags,
    color_id,
    category_id,
    author_id,
    created_at_unix,
    updated_at_unix,
    comment_ref_offset
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

export function readWordDictHeader(view) {
  const magic = readMagic(view, 0);
  const version = view.getUint32(4, true);
  const entry_count_lo = view.getUint32(8, true);
  const entry_count_hi = view.getUint32(12, true);
  const entry_count = BigInt(entry_count_lo) | (BigInt(entry_count_hi) << 32n);
  return { magic, version, entry_count };
}

export function readWordDictEntry(view, baseOffsetBytes, index) {
  const size = 24;
  const off = baseOffsetBytes + index * size;
  const word_hash = readU64(view, off + 0);
  const postings_offset = readU64(view, off + 8);
  const postings_count = readU64(view, off + 16);
  return { word_hash, postings_offset, postings_count };
}

export function wordDictBinarySearch(view, targetHash) {
  const hdr = readWordDictHeader(view);
  if (hdr.magic !== "WDIX" || hdr.version !== 1) {
    throw new Error("invalid WDIX header");
  }
  let lo = 0n;
  let hi = hdr.entry_count;
  const base = 16;

  while (lo < hi) {
    const mid = lo + (hi - lo) / 2n;
    const midIndex = Number(mid);
    const entry = readWordDictEntry(view, base, midIndex);
    const h = entry.word_hash;
    if (h === targetHash) {
      return entry;
    } else if (h < targetHash) {
      lo = mid + 1n;
    } else {
      hi = mid;
    }
  }

  return null;
}

export async function loadWordDictHeaderRange(url) {
  const view = await fetchRangeAsDataView(url, 0n, 16n);
  return readWordDictHeader(view);
}

export async function loadWordDictEntryRange(url, index) {
  const start = 16n + BigInt(index) * 24n;
  const view = await fetchRangeAsDataView(url, start, 24n);
  return readWordDictEntry(view, 0, 0);
}

export function parsePostingsList(view, offsetBytes) {
  let offset = offsetBytes;
  const docCount = view.getUint32(offset, true);
  offset += 4;
  const entries = [];
  let currentPageId = 0;

  for (let i = 0; i < docCount; i++) {
    const docDelta = view.getUint32(offset, true);
    offset += 4;
    const termFreq = view.getUint32(offset, true);
    offset += 4;
    currentPageId += docDelta;
    const positions = [];
    let pos = 0;
    for (let j = 0; j < termFreq; j++) {
      const d = view.getUint32(offset, true);
      offset += 4;
      pos += d;
      positions.push(pos);
    }
    entries.push({
      page_id: currentPageId,
      term_freq: termFreq,
      positions
    });
  }

  return {
    entries,
    docCount,
    nextOffset: offset
  };
}

export function parsePostingsPages(view, offsetBytes) {
  const list = parsePostingsList(view, offsetBytes);
  return list.entries.map(e => e.page_id);
}

export async function readPostingsPages(url, postingsOffset) {
  const key = url + ":" + String(postingsOffset);
  if (postingsPagesCache.has(key)) {
    return postingsPagesCache.get(key);
  }
  const view = await loadBinaryAsDataView(url);
  const hdr = readPostingsHeader(view);
  if (hdr.flags & 1) {
    throw new Error("compressed postings not supported in JS reader");
  }
  const base = Number(postingsOffset);
  const blockSize = view.getUint32(base, true);
  const pages = parsePostingsPages(view, base + 4);
  postingsPagesCache.set(key, pages);
  return pages;
}

export async function readPostingsPagesRange(url, postingsOffset) {
  const headerView = await fetchRangeAsDataView(url, postingsOffset, 4n);
  const blockSize = headerView.getUint32(0, true);
  const dataView = await fetchRangeAsDataView(
    url,
    postingsOffset + 4n,
    BigInt(blockSize)
  );
  return parsePostingsPages(dataView, 0);
}

export function findPageIndexEntry(view, pageId) {
  const header = readPagesIndexHeader(view);
  const base = 24;
  for (let i = 0; i < header.pageCount; i++) {
    const e = readPageIndexEntry(view, base, i);
    if (e.page_id === pageId) {
      return e;
    }
  }
  return null;
}

export async function readPageText(booksUrl, pagesIdxUrl, pageId) {
  const cacheKey = booksUrl + "|" + pagesIdxUrl + "|" + String(pageId);
  if (pageTextCache.has(cacheKey)) {
    return pageTextCache.get(cacheKey);
  }
  const pagesView = await loadBinaryAsDataView(pagesIdxUrl);
  const pe = findPageIndexEntry(pagesView, pageId);
  if (!pe) {
    pageTextCache.set(cacheKey, null);
    return null;
  }

  const booksView = await loadBinaryAsDataView(booksUrl);
  const booksHeader = readBooksHeader(booksView);
  const blockIndexBase = Number(booksHeader.indexOffset);
  const blockEntry = readBlockIndexEntry(booksView, blockIndexBase, pe.block_id);

  const start = Number(blockEntry.compressedOffset + BigInt(pe.offset_in_block));
  const bytes = new Uint8Array(
    booksView.buffer,
    booksView.byteOffset + start,
    pe.length
  );
  const text = jhTextDecoder.decode(bytes);

  const result = {
    page_id: pe.page_id,
    book_id: pe.book_id,
    chapter_id: pe.chapter_id,
    page_number: pe.page_number,
    text
  };
  pageTextCache.set(cacheKey, result);
  return result;
}

export async function readPageTextRange(booksUrl, pagesIdxUrl, pageId) {
  const pagesView = await loadBinaryAsDataView(pagesIdxUrl);
  const pe = findPageIndexEntry(pagesView, pageId);
  if (!pe) {
    return null;
  }
  const headerView = await fetchRangeAsDataView(booksUrl, 0n, 36n);
  const booksHeader = readBooksHeader(headerView);
  const blockIndexBase = booksHeader.indexOffset;
  const blockEntryOffset = blockIndexBase + BigInt(pe.block_id) * 32n;
  const blockEntryView = await fetchRangeAsDataView(booksUrl, blockEntryOffset, 32n);
  const blockEntry = readBlockIndexEntry(blockEntryView, 0, 0);
  const start = blockEntry.compressedOffset + BigInt(pe.offset_in_block);
  const bytes = await fetchRangeAsUint8Array(booksUrl, start, BigInt(pe.length));
  const text = jhTextDecoder.decode(bytes);
  return {
    page_id: pe.page_id,
    book_id: pe.book_id,
    chapter_id: pe.chapter_id,
    page_number: pe.page_number,
    text
  };
}

export function hashUtf8_64(str, seed = 0n) {
  const fnv_offset = 14695981039346656037n;
  const fnv_prime = 1099511628211n;
  if (seed === 0n) {
    const cached = jhHashCache.get(str);
    if (cached !== undefined) {
      return cached;
    }
  }
  let h = fnv_offset ^ seed;
  const bytes = jhTextEncoder.encode(str);
  for (let i = 0; i < bytes.length; i++) {
    h ^= BigInt(bytes[i]);
    h = (h * fnv_prime) & 0xffffffffffffffffn;
  }
  h ^= h >> 33n;
  h = (h * 0xff51afd7ed558ccdn) & 0xffffffffffffffffn;
  h ^= h >> 33n;
  h = (h * 0xc4ceb9fe1a85ec53n) & 0xffffffffffffffffn;
  h ^= h >> 33n;
  if (seed === 0n) {
    if (jhHashCache.size >= JH_HASH_CACHE_LIMIT) {
      const it = jhHashCache.keys().next();
      if (!it.done) {
        jhHashCache.delete(it.value);
      }
    }
    jhHashCache.set(str, h);
  }
  return h;
}

function tokenizeQuerySimple(query) {
  return query
    .split(/\s+/)
    .map(t => t.trim())
    .filter(t => t.length > 0);
}

function findPostingInEntries(entries, pageId) {
  let lo = 0;
  let hi = entries.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    const v = entries[mid].page_id;
    if (v === pageId) {
      return entries[mid];
    } else if (v < pageId) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return null;
}

function phraseMatchesDoc(docEntries) {
  if (!docEntries || docEntries.length === 0) {
    return false;
  }
  const base = docEntries[0];
  for (let i = 0; i < base.positions.length; i++) {
    const pos0 = base.positions[i];
    let ok = true;
    for (let t = 1; t < docEntries.length; t++) {
      const pe = docEntries[t];
      const want = pos0 + t;
      const pb = pe.positions;
      let lo = 0;
      let hi = pb.length;
      let found = false;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        const v = pb[mid];
        if (v === want) {
          found = true;
          break;
        } else if (v < want) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      if (!found) {
        ok = false;
        break;
      }
    }
    if (ok) {
      return true;
    }
  }
  return false;
}

function makeRun(start, end, style) {
  return {
    start,
    end,
    bold: !!style.bold,
    italic: !!style.italic,
    underline: !!style.underline,
    smallCaps: !!style.smallCaps,
    colorId: style.colorId || 0,
    highlightCategoryId: style.highlightCategoryId || 0
  };
}

function buildEventsForFormatting(formatEntries) {
  const events = [];
  for (let i = 0; i < formatEntries.length; i++) {
    const e = formatEntries[i];
    const style = {
      bold: !!(e.flags & 1),
      italic: !!(e.flags & 2),
      underline: !!(e.flags & 4),
      smallCaps: !!(e.flags & 8),
      styleId: e.style_id,
      layer: e.layer,
      priority: e.priority
    };
    events.push({ pos: e.anchor_start, type: "start-format", style });
    events.push({ pos: e.anchor_end, type: "end-format", style });
  }
  return events;
}

function buildEventsForHighlights(highlightEntries) {
  const events = [];
  for (let i = 0; i < highlightEntries.length; i++) {
    const e = highlightEntries[i];
    if (e.flags & 1) {
      continue;
    }
    const style = {
      highlight: true,
      colorId: e.color_id,
      highlightCategoryId: e.category_id
    };
    events.push({ pos: e.anchor_start, type: "start-highlight", style });
    events.push({ pos: e.anchor_end, type: "end-highlight", style });
  }
  return events;
}

export function mergeFormattingAndHighlights(tokenCount, formatEntries, highlightEntries) {
  const events = [];
  events.push(...buildEventsForFormatting(formatEntries));
  events.push(...buildEventsForHighlights(highlightEntries));
  events.sort((a, b) => {
    if (a.pos !== b.pos) {
      return a.pos - b.pos;
    }
    const order = {
      "end-format": 0,
      "end-highlight": 1,
      "start-format": 2,
      "start-highlight": 3
    };
    return order[a.type] - order[b.type];
  });
  const activeFormatting = [];
  const activeHighlights = [];
  function currentStyle() {
    let bold = false;
    let italic = false;
    let underline = false;
    let smallCaps = false;
    let colorId = 0;
    let highlightCategoryId = 0;
    if (activeFormatting.length > 0) {
      const top = activeFormatting[activeFormatting.length - 1];
      bold = !!top.bold;
      italic = !!top.italic;
      underline = !!top.underline;
      smallCaps = !!top.smallCaps;
    }
    if (activeHighlights.length > 0) {
      const last = activeHighlights[activeHighlights.length - 1];
      colorId = last.colorId;
      highlightCategoryId = last.highlightCategoryId;
    }
    return { bold, italic, underline, smallCaps, colorId, highlightCategoryId };
  }
  const runs = [];
  let curStart = 0;
  let curStyle = currentStyle();
  let eventIndex = 0;
  for (let pos = 0; pos <= tokenCount; pos++) {
    while (eventIndex < events.length && events[eventIndex].pos === pos) {
      const ev = events[eventIndex];
      if (ev.type === "start-format") {
        const style = ev.style;
        let inserted = false;
        for (let i = 0; i < activeFormatting.length; i++) {
          const s = activeFormatting[i];
          if (style.layer < s.layer || (style.layer === s.layer && style.priority <= s.priority)) {
            activeFormatting.splice(i, 0, style);
            inserted = true;
            break;
          }
        }
        if (!inserted) {
          activeFormatting.push(style);
        }
      } else if (ev.type === "end-format") {
        const idx = activeFormatting.indexOf(ev.style);
        if (idx >= 0) {
          activeFormatting.splice(idx, 1);
        }
      } else if (ev.type === "start-highlight") {
        activeHighlights.push(ev.style);
      } else if (ev.type === "end-highlight") {
        const idx = activeHighlights.indexOf(ev.style);
        if (idx >= 0) {
          activeHighlights.splice(idx, 1);
        }
      }
      eventIndex++;
    }
    if (pos === 0) {
      curStyle = currentStyle();
      continue;
    }
    const nextStyle = currentStyle();
    const styleChanged =
      curStyle.bold !== nextStyle.bold ||
      curStyle.italic !== nextStyle.italic ||
      curStyle.underline !== nextStyle.underline ||
      curStyle.smallCaps !== nextStyle.smallCaps ||
      curStyle.colorId !== nextStyle.colorId ||
      curStyle.highlightCategoryId !== nextStyle.highlightCategoryId;
    if (styleChanged || pos === tokenCount) {
      if (pos > curStart) {
        runs.push(makeRun(curStart, pos, curStyle));
      }
      curStart = pos;
      curStyle = nextStyle;
    }
  }
  return runs;
}

export async function browserPhraseSearch(wordsIdxUrl, postingsUrl, query) {
  const tokens = tokenizeQuerySimple(query);
  if (tokens.length === 0) {
    return [];
  }

  const dictView = await loadBinaryAsDataView(wordsIdxUrl);
  const postingsView = await loadBinaryAsDataView(postingsUrl);
  const hdr = readPostingsHeader(postingsView);
  if (hdr.flags & 1) {
    throw new Error("compressed postings not supported in JS reader");
  }

  const hashes = tokens.map(t => hashUtf8_64(t));
  const lists = [];

  for (let i = 0; i < hashes.length; i++) {
    const entry = wordDictBinarySearch(dictView, hashes[i]);
    if (!entry || entry.postings_count === 0n) {
      return [];
    }
    const key = postingsUrl + ":" + String(entry.postings_offset);
    let list = postingsListCache.get(key);
    if (!list) {
      const base = Number(entry.postings_offset);
      const blockSize = postingsView.getUint32(base, true);
      list = parsePostingsList(postingsView, base + 4);
      postingsListCache.set(key, list);
    }
    lists.push(list);
  }

  if (lists.length === 1) {
    return lists[0].entries.map(e => e.page_id);
  }

  let baseIdx = 0;
  for (let i = 1; i < lists.length; i++) {
    if (lists[i].docCount < lists[baseIdx].docCount) {
      baseIdx = i;
    }
  }

  const baseList = lists[baseIdx];
  const resultPages = [];

  for (let i = 0; i < baseList.entries.length; i++) {
    const baseEntry = baseList.entries[i];
    const d = baseEntry.page_id;
    const docEntries = new Array(lists.length);
    docEntries[baseIdx] = baseEntry;
    let okAll = true;
    for (let t = 0; t < lists.length; t++) {
      if (t === baseIdx) {
        continue;
      }
      const e = findPostingInEntries(lists[t].entries, d);
      if (!e) {
        okAll = false;
        break;
      }
      docEntries[t] = e;
    }
    if (!okAll) {
      continue;
    }
    const ordered = new Array(lists.length);
    for (let t = 0; t < lists.length; t++) {
      ordered[t] = docEntries[t];
    }
    if (phraseMatchesDoc(ordered)) {
      resultPages.push(d);
    }
  }

  return resultPages;
}
