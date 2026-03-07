#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <tuple>

namespace rocksdb {
class DB;
}

class LsmStore {
public:
    explicit LsmStore(const std::string& db_path);
    ~LsmStore();

    LsmStore(const LsmStore&) = delete;
    LsmStore& operator=(const LsmStore&) = delete;

    LsmStore(LsmStore&&) noexcept;
    LsmStore& operator=(LsmStore&&) noexcept;

    std::optional<uint32_t> getDocIdByPath(const std::string& path) const;
    void putPath(const std::string& path, uint32_t doc_id);

    void putDocMeta(uint32_t doc_id, const std::string& title, const std::string& path);
    std::optional<std::tuple<std::string, std::string>> getDocMeta(uint32_t doc_id) const;

    void tombstone(uint32_t doc_id);
    bool isDeleted(uint32_t doc_id) const;

    std::optional<std::string> getHashByPath(const std::string& path) const;
    void putHashForPath(const std::string& path, const std::string& hex_hash);

private:
    rocksdb::DB* db_{nullptr};

    static std::string kPath(const std::string& path);
    static std::string kDocMeta(uint32_t doc_id);
    static std::string kTomb(uint32_t doc_id);

    static std::string u32le(uint32_t v);
    static std::optional<uint32_t> read_u32le(const std::string& s);

    static std::string packMeta(const std::string& title, const std::string& path);
    static std::optional<std::tuple<std::string, std::string>> unpackMeta(const std::string& blob);

    static std::string kHash(const std::string& path);
};