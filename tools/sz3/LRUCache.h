#pragma once
#include <SZ3/api/sz.hpp>
#include <list>
#include <unordered_map>

constexpr int NUM_PARTITION = 10;
constexpr int NUM_COL = 5;
constexpr int PARTITION_SIZE = 200000;
const std::vector<int> PARTITION_END_IDX = {200000, 400000, 600000, 800000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000};
const std::string cmpFileDir = "../../../dl-retrieval/outputs/compressed_files/";
const std::string indiceDir = "../../../dl-retrieval/outputs/indices/";
std::vector<std::string> colNames = {"dst", "hist", "enumber", "etime", "rnumber"};

template <class T>
T *decompress(const char *cmpPath, SZ::Config &conf) {
    size_t cmpSize;
    auto cmpData = SZ::readfile<char>(cmpPath, cmpSize);
    T *decData = SZ_decompress<T>(conf, cmpData.get(), cmpSize);
    return decData;
}

class LRUCache {

  private:
    int capacity;
    std::list<int> cache; // stores partition ids
    std::unordered_map<int, std::list<int>::iterator> map_cache_to_position;
    std::unordered_map<int, std::vector<float *>> map_cache_to_data;
    SZ::Config conf;
    int cache_misses{0};

  public:
    LRUCache(int capacity_, SZ::Config conf_) : capacity{capacity_}, conf{conf_} {}

    int get_cache_misses() {
        return cache_misses;
    }
    // This function returns false if key is not
    // present in cache. Else it moves the key to
    // front by first removing it and then adding
    // it, and returns true.
    bool get(int key) {
        auto it = map_cache_to_position.find(key);
        if (it == map_cache_to_position.end()) {
            return false;
        }
        cache.splice(cache.end(), cache, it->second);
        return true;
    }

    std::vector<float *> refer(int key) {
        // std::cout << "Key: " << key << std::endl;
        if (!get(key)) {
            put(key);
        }
        return map_cache_to_data[key];
    }

    void put(int key) {
        cache_misses++;
        // std::cout << "Cache miss!!!" << std::endl;
        if (cache.size() == capacity) {
            int first_key = cache.front();
            cache.pop_front();
            map_cache_to_position.erase(first_key);
            // Free memory
            for (float *data : map_cache_to_data[first_key]) {
                delete[] data;
            }
            map_cache_to_data.erase(first_key);
        }
        cache.push_back(key);
        map_cache_to_position[key] = --cache.end();

        // Decompress and store data
        std::vector<float *> data;
        data.reserve(NUM_COL);
        for (int col = 0; col < NUM_COL; ++col) {
            std::string colName = colNames[col];
            int end = PARTITION_END_IDX[key];
            int start = end - PARTITION_SIZE;
            std::string partionName = cmpFileDir + colName + "-" + std::to_string(start) + "-" + std::to_string(end) + ".sz";
            float *colValues = decompress<float>(partionName.c_str(), conf);
            data.emplace_back(colValues);
        }
        map_cache_to_data[key] = data;
    }

    // Displays contents of cache in Reverse Order
    void display() {
        for (auto it = cache.rbegin(); it != cache.rend(); ++it) {
            std::cout << *it << " ";
        }
    }
};