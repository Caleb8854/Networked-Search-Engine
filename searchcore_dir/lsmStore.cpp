#include "lsmStore.hpp"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <cstring>
#include <stdexcept>
#include <utility>

static inline void must_ok(const rocksdb::Status& st) {
    if (!st.ok()) throw std::runtime_error(st.ToString());
}

LsmStore::LsmStore(const std::string& db_path) {
    rocksdb::Options opt;
    opt.create_if_missing = true;

    opt.IncreaseParallelism();
    opt.OptimizeLevelStyleCompaction();
    opt.max_open_files = 256;

    must_ok(rocksdb::DB::Open(opt, db_path, &db_));
}

LsmStore::~LsmStore() {
    delete db_;
    db_ = nullptr;
}

LsmStore::LsmStore(LsmStore&& other) noexcept {
    db_ = other.db_;
    other.db_ = nullptr;
}

LsmStore& LsmStore::operator=(LsmStore&& other) noexcept {
    if (this == &other) return *this;
    delete db_;
    db_ = other.db_;
    other.db_ = nullptr;
    return *this;
}

std::string LsmStore::kPath(const std::string& path) {
    return std::string("p:") + path;
}

std::string LsmStore::kDocMeta(uint32_t doc_id) {
    std::string k = "d:";
    k += u32le(doc_id);
    return k;
}

std::string LsmStore::kTomb(uint32_t doc_id) {
    std::string k = "x:";
    k += u32le(doc_id);
    return k;
}

std::string LsmStore::u32le(uint32_t v) {
    std::string out(4, '\0');
    out[0] = static_cast<char>(v & 0xFF);
    out[1] = static_cast<char>((v >> 8) & 0xFF);
    out[2] = static_cast<char>((v >> 16) & 0xFF);
    out[3] = static_cast<char>((v >> 24) & 0xFF);
    return out;
}

std::optional<uint32_t> LsmStore::read_u32le(const std::string& s) {
    if (s.size() != 4) return std::nullopt;
    uint32_t v = (uint8_t)s[0]
               | ((uint8_t)s[1] << 8)
               | ((uint8_t)s[2] << 16)
               | ((uint8_t)s[3] << 24);
    return v;
}

std::string LsmStore::packMeta(const std::string& title, const std::string& path) {
    std::string out;
    out.reserve(8 + title.size() + path.size());
    out += u32le(static_cast<uint32_t>(title.size()));
    out += title;
    out += u32le(static_cast<uint32_t>(path.size()));
    out += path;
    return out;
}

std::optional<std::tuple<std::string, std::string>> LsmStore::unpackMeta(const std::string& blob) {
    auto read_u32_at = [&](size_t off) -> std::optional<uint32_t> {
        if (off + 4 > blob.size()) return std::nullopt;
        uint32_t v = (uint8_t)blob[off]
                   | ((uint8_t)blob[off + 1] << 8)
                   | ((uint8_t)blob[off + 2] << 16)
                   | ((uint8_t)blob[off + 3] << 24);
        return v;
    };

    size_t off = 0;
    auto tlen = read_u32_at(off);
    if (!tlen) return std::nullopt;
    off += 4;
    if (off + *tlen > blob.size()) return std::nullopt;
    std::string title(blob.data() + off, *tlen);
    off += *tlen;

    auto plen = read_u32_at(off);
    if (!plen) return std::nullopt;
    off += 4;
    if (off + *plen > blob.size()) return std::nullopt;
    std::string path(blob.data() + off, *plen);

    return std::make_tuple(std::move(title), std::move(path));
}

std::optional<uint32_t> LsmStore::getDocIdByPath(const std::string& path) const {
    std::string value;
    auto st = db_->Get(rocksdb::ReadOptions(), kPath(path), &value);
    if (!st.ok()) return std::nullopt;
    return read_u32le(value);
}

void LsmStore::putPath(const std::string& path, uint32_t doc_id) {
    must_ok(db_->Put(rocksdb::WriteOptions(), kPath(path), u32le(doc_id)));
}

void LsmStore::putDocMeta(uint32_t doc_id, const std::string& title, const std::string& path) {
    must_ok(db_->Put(rocksdb::WriteOptions(), kDocMeta(doc_id), packMeta(title, path)));
}

std::optional<std::tuple<std::string, std::string>> LsmStore::getDocMeta(uint32_t doc_id) const {
    std::string value;
    auto st = db_->Get(rocksdb::ReadOptions(), kDocMeta(doc_id), &value);
    if (!st.ok()) return std::nullopt;
    return unpackMeta(value);
}

void LsmStore::tombstone(uint32_t doc_id) {
    must_ok(db_->Put(rocksdb::WriteOptions(), kTomb(doc_id), "1"));
}

bool LsmStore::isDeleted(uint32_t doc_id) const {
    std::string value;
    auto st = db_->Get(rocksdb::ReadOptions(), kTomb(doc_id), &value);
    return st.ok();
}

std::string LsmStore::kHash(const std::string& path) {
    return std::string("h:") + path;
}

std::optional<std::string> LsmStore::getHashByPath(const std::string& path) const {
    std::string value;
    auto st = db_->Get(rocksdb::ReadOptions(), kHash(path), &value);
    if (!st.ok()) return std::nullopt;
    return value;
}

void LsmStore::putHashForPath(const std::string& path, const std::string& hex_hash) {
    must_ok(db_->Put(rocksdb::WriteOptions(), kHash(path), hex_hash));
}

uint32_t LsmStore::allocDocIdBlock(uint32_t n) {
    const std::string key = "meta:nextDocId";

    std::string value;
    uint32_t current = 1;

    auto st = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (st.ok() && value.size() == sizeof(uint32_t)) {
        std::memcpy(&current, value.data(), sizeof(uint32_t));
    }

    uint32_t next = current + n;

    std::string out(sizeof(uint32_t), '\0');
    std::memcpy(out.data(), &next, sizeof(uint32_t));

    must_ok(db_->Put(rocksdb::WriteOptions(), key, out));

    return current;
}

uint32_t LsmStore::peekNextDocId() const {
    const std::string key = "meta:nextDocId";

    std::string value;
    uint32_t current = 1;

    auto st = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (st.ok() && value.size() == sizeof(uint32_t)) {
        std::memcpy(&current, value.data(), sizeof(uint32_t));
    }
    return current;
}

void LsmStore::upsertDoc(const std::string& path, const std::string& title, uint32_t new_doc_id, const std::string& sha256_hex, std::optional<uint32_t> old_doc_id) {
    rocksdb::WriteBatch wb;
    if(old_doc_id.has_value()) {
        wb.Put(kTomb(old_doc_id.value()), "1");
    }
    wb.Put(kPath(path), u32le(new_doc_id));
    wb.Put(kDocMeta(new_doc_id), packMeta(title, path));
    wb.Put(kHash(path), sha256_hex);
    rocksdb::WriteOptions wo;
    must_ok(db_->Write(wo, &wb));
}