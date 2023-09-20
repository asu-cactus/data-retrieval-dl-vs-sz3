#include "LRUCache.h"

std::vector<int> load_indices(const char *path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        std::cout << "Failed to open file: " << path << std::endl;
        exit(1);
    }
    std::string str;
    std::vector<int> indices;
    while (std::getline(file, str)) {
        indices.push_back(std::stoi(str));
    }
    return indices;
}

std::pair<std::vector<int>, std::vector<int>> get_partitions_and_ids(std::vector<int> &all_ids) {
    std::vector<int> partitionIds;
    std::vector<int> partitionOffsets;
    partitionIds.reserve(all_ids.size());
    partitionOffsets.reserve(all_ids.size());
    // Find which partition each id belongs to
    for (int id : all_ids) {
        // Find lower bound of id in vector PARTITION_END_IDX
        int partitionId = std::upper_bound(PARTITION_END_IDX.begin(), PARTITION_END_IDX.end(), id) - PARTITION_END_IDX.begin();
        partitionIds.emplace_back(partitionId);
        // Generate integer array in range [start, end)
        int start = partitionId * 20000;
        std::vector<int> indexPartition(PARTITION_SIZE);
        std::iota(indexPartition.begin(), indexPartition.end(), start);
        // Find the offset of the id in the partition
        int offset = std::upper_bound(indexPartition.begin(), indexPartition.end(), id) - indexPartition.begin();
        partitionOffsets.emplace_back(offset);
    }
    // std::cout << "Partitions:\n";
    // for (int i = 0; i < partitionIds.size(); ++i) {
    //     std::cout << partitionIds[i] << " ";
    // }
    // std::cout << std::endl;
    return std::make_pair(partitionIds, partitionOffsets);
}

float *retrieve(
    const char *cmpPath,
    std::vector<int> &offsets,
    SZ::Config &conf,
    double &decompress_time,
    double &access_time) {
    // Start timer
    SZ::Timer timer(true);

    // Decompress data
    std::vector<float> decData = decompress<float>(cmpPath, conf);
    double decompress_end_time = timer.stop();
    decompress_time += decompress_end_time;

    // Look up ids from decData using binary search
    float *results = new float[offsets.size()];
    for (int i = 0; i < offsets.size(); i++) {
        results[i] = decData[offsets[i]];
    }
    access_time += timer.stop() - decompress_end_time;
    // delete[] decData;
    return results;
}

float retrieve_single(
    const char *cmpPath,
    int offset,
    SZ::Config &conf,
    double &decompress_time,
    double &access_time) {
    // Start timer
    SZ::Timer timer(true);
    // Decompress data
    std::vector<float> decData = decompress<float>(cmpPath, conf);
    double decompress_end_time = timer.stop();
    decompress_time += decompress_end_time;

    // Look up ids from decData using binary search
    float result = decData[offset];
    access_time += timer.stop() - decompress_end_time;
    // delete[] decData;
    return result;
}

void upper_bound(
    std::vector<int> &partitions,
    std::vector<int> &partitionOffsets,
    SZ::Config &conf,
    double &decompress_time,
    double &access_time) {
    // For each partition file, decompress it and look up the data, then merge the results
    std::vector<float *> results;
    results.reserve(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
        // Get offsets to query for this partition
        int offset = partitionOffsets[i];
        int partitionID = partitions[i];
        // Decompress and look up data for each column
        float row[NUM_COL];
        for (int col = 0; col < NUM_COL; ++col) {
            std::string colName = colNames[col];
            int end = PARTITION_END_IDX[partitionID];
            int start = end - PARTITION_SIZE;
            std::string partionName = cmpFileDir + colName + "-" + std::to_string(start) + "-" + std::to_string(end) + ".sz";
            row[col] = retrieve_single(partionName.c_str(), offset, conf, decompress_time, access_time);
        }
        results.emplace_back(row);
    }
}

void lower_bound(
    std::vector<int> &partitions,
    std::vector<int> &partitionOffsets,
    SZ::Config &conf,
    double &decompress_time,
    double &access_time) {
    // For lower bound, we assume all ids are in the 1st partition
    int partitionID = partitions[0];
    std::vector<float *> results;
    results.reserve(NUM_COL);
    // Decompress and look up data for each column
    for (int col = 0; col < NUM_COL; ++col) {
        std::string colName = colNames[col];
        int end = PARTITION_END_IDX[partitionID];
        int start = end - PARTITION_SIZE;
        std::string partionName = cmpFileDir + colName + "-" + std::to_string(start) + "-" + std::to_string(end) + ".sz";
        float *colValues = retrieve(partionName.c_str(), partitionOffsets, conf, decompress_time, access_time);
        results.emplace_back(colValues);
    }
    // Clean up results
    for (float *result : results) {
        delete[] result;
    }
}

std::vector<std::pair<int, int>> get_end_offsets(std::vector<int> &partitions) {
    // Get the end indices of each partition, and store as (partitionID, end_index) pairs
    std::vector<std::pair<int, int>> end_indices;
    for (auto itr = partitions.begin(); itr != partitions.end(); ++itr) {
        if (*itr != *(itr + 1)) {
            end_indices.push_back(std::make_pair(*itr, itr - partitions.begin()));
        }
    }
    return end_indices;
}

void print_vector(std::vector<std::pair<int, int>> &v) {
    for (auto ele : v) {
        std::cout << ele.first << "," << ele.second << "\n";
    }
}

void general_query(
    std::vector<int> &partitions,
    std::vector<int> &partitionOffsets,
    LRUCache &cache,
    double &decompress_time,
    double &access_time) {

    // Print partitions
    std::vector<std::pair<int, int>> end_indices = get_end_offsets(partitions);
    // std::cout << "End indices:\n";
    // print_vector(end_indices);

    int start_index = 0;
    for (std::pair<int, int> pair : end_indices) {
        int partitionID = pair.first;
        int end_index = pair.second;

        // Obtain data from cache that may involve decompression
        SZ::Timer timer(true);
        std::vector<std::vector<float>> data = cache.refer(partitionID);
        decompress_time += timer.stop();

        // Access data
        timer.start();
        std::vector<float *> results;
        results.reserve(NUM_COL);
        for (std::vector<float> colValues : data) {
            float *result = new float[end_index - start_index + 1];
            for (int i = start_index; i <= end_index; ++i) {
                result[i - start_index] = colValues[partitionOffsets[i]];
            }
            results.emplace_back(result);
        }
        start_index = end_index;
        access_time += timer.stop();

        // Free memory for results
        for (float *result : results) {
            delete[] result;
        }
    }
}

int main(int argc, char *argv[]) {
    int cache_size = std::stoi(argv[1]);
    std::string indiceFile = argv[2];

    // Get configurations
    constexpr size_t dims[1] = {PARTITION_SIZE};
    SZ::Config conf({dims[0]});
    conf.cmprAlgo = SZ::ALGO_INTERP_LORENZO;
    conf.errorBoundMode = SZ::EB_REL; // refer to def.hpp for all supported error bound mode
    conf.relErrorBound = 1E-4;        // rel error bound 1e-4
    conf.openmp = false;

    // Start timer
    SZ::Timer timer(true);

    // Load indices and get partition file names and ids
    std::vector<int> all_ids = load_indices((indiceDir + indiceFile).c_str());
    auto [partitions, partitionOffsets] = get_partitions_and_ids(all_ids);
    double search_time = timer.stop();

    // Run query
    double decompress_time = 0.0;
    double access_time = 0.0;

    LRUCache cache(cache_size, conf);
    general_query(partitions, partitionOffsets, cache, decompress_time, access_time);
    // upper_bound(partitions, partitionOffsets, conf, decompress_time, access_time);
    // lower_bound(partitions, partitionOffsets, conf, decompress_time, access_time);

    // Stop timer and measure overall retrieval time
    double overall_time = timer.stop();

    printf("Cache miss rate: %d.\n", cache.get_cache_misses());
    printf("Search time = %f seconds.\n", search_time);
    printf("Decompression time = %f seconds.\n", decompress_time);
    printf("Access time = %f seconds.\n", access_time);
    printf("Overall retrieval time = %f seconds.\n", overall_time);
    return 0;
}