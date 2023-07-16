#include <SZ3/api/sz.hpp>

constexpr int NUM_PARTITION = 10;
constexpr int NUM_COL = 5;
constexpr int PARTITION_SIZE = 200000;
const std::vector<int> index_start = {0, 200000, 400000, 600000, 800000, 1000000, 1200000, 1400000, 1600000, 1800000};
const std::string cmpFileDir = "../../../dl-retrieval/outputs/compressed_files/";
const std::string indiceDir = "../../../dl-retrieval/outputs/indices/";
std::vector<std::string> colNames = {"dst", "hist", "enumber", "etime", "rnumber"};

template <class T>
T *decompress(const char *cmpPath, SZ::Config conf) {
    size_t cmpSize;
    auto cmpData = SZ::readfile<char>(cmpPath, cmpSize);

    // SZ::Timer timer(true);
    T *decData = SZ_decompress<T>(conf, cmpData.get(), cmpSize);
    // double compress_time = timer.stop();

    // printf("compression ratio = %f\n", conf.num * sizeof(T) * 1.0 / cmpSize);
    // printf("decompression time = %f seconds.\n", compress_time);
    return decData;
}

int binary_search_find_index(std::vector<int> v, int data) {
    int start = 0;
    int end = v.size() - 1;
    while (start <= end) {
        int mid = (start + end) / 2;
        if (v[mid] == data) {
            return mid;
        } else if (v[mid] < data) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
    }
    return -1; // Not found
}

std::pair<std::vector<int>, std::vector<int>> get_partitions_and_ids(std::vector<int> &all_ids) {
    std::vector<int> partitionIds;
    std::vector<int> partitionOffsets;
    partitionIds.reserve(all_ids.size());
    partitionOffsets.reserve(all_ids.size());
    // Find which partition each id belongs to
    for (int id : all_ids) {
        int partitionId = binary_search_find_index(index_start, 0);
        partitionIds.push_back(partitionId);
        // Generate integer array in range [start, end)
        int start = partitionId * 20000;
        std::vector<int> indexPartition(PARTITION_SIZE);
        std::iota(indexPartition.begin(), indexPartition.end(), start);
        // Find the offset of the id in the partition
        int offset = binary_search_find_index(indexPartition, id);
        partitionOffsets.push_back(offset);
    }
    return std::make_pair(partitionIds, partitionOffsets);
}

float *retrieve(
    const char *cmpPath,
    std::vector<int> &offsets,
    SZ::Config &conf,
    double &decompress_time,
    double &lookup_time) {
    // Start timer
    SZ::Timer timer(true);

    // Decompress data
    float *decData = decompress<float>(cmpPath, conf);
    double decompress_end_time = timer.stop();
    decompress_time += decompress_end_time;

    // Look up ids from decData using binary search
    float *results = new float[offsets.size()];
    for (int i = 0; i < offsets.size(); i++) {
        results[i] = decData[offsets[i]];
    }
    lookup_time += timer.stop() - decompress_end_time;
    return results;
}

float retrieve_single(const char *cmpPath, int offset, SZ::Config &conf, double &decompress_time, double &lookup_time) {
    // Start timer
    SZ::Timer timer(true);
    // Decompress data
    float *decData = decompress<float>(cmpPath, conf);
    double decompress_end_time = timer.stop();
    decompress_time += decompress_end_time;

    // Look up ids from decData using binary search
    float result = decData[offset];
    lookup_time += timer.stop() - decompress_end_time;
    return result;
}

std::vector<int> load_indices(const char *path) {
    std::ifstream file(path);
    std::string str;
    std::vector<int> indices;
    while (std::getline(file, str)) {
        indices.push_back(std::stoi(str));
    }

    return indices;
}

void upper_bound(
    std::vector<int> &partitions,
    std::vector<int> &partitionOffsets,
    std::vector<std::string> &colNames,
    SZ::Config &conf,
    double &decompress_time,
    double &lookup_time) {
    // For each partition file, decompress it and look up the data, then merge the results
    for (int i = 0; i < partitions.size(); i++) {
        // Get offsets to query for this partition
        int offset = partitionOffsets[i];
        int partitionID = partitions[i];
        // Decompress and look up data for each column
        float row[NUM_COL];
        for (int col = 0; col < NUM_COL; ++col) {
            std::string colName = colNames[col];
            int start = index_start[partitionID];
            int end = start + PARTITION_SIZE;
            std::string partionName = cmpFileDir + colName + "-" + std::to_string(start) + "-" + std::to_string(end) + ".sz";
            row[col] = retrieve_single(partionName.c_str(), offset, conf, decompress_time, lookup_time);
        }
    }
}

void lower_bound(
    std::vector<int> &partitions,
    std::vector<int> &partitionOffsets,
    std::vector<std::string> &colNames,
    SZ::Config &conf,
    double &decompress_time,
    double &lookup_time) {
    // For lower bound, we assume all ids are in the 1st partition
    int partitionID = partitions[0];
    std::vector<float *> results(NUM_COL);
    // Decompress and look up data for each column
    for (int col = 0; col < NUM_COL; ++col) {
        std::string colName = colNames[col];
        int start = index_start[partitionID];
        int end = start + PARTITION_SIZE;
        std::string partionName = cmpFileDir + colName + "-" + std::to_string(start) + "-" + std::to_string(end) + ".sz";
        float *colValues = retrieve(partionName.c_str(), partitionOffsets, conf, decompress_time, lookup_time);
        results[col] = colValues;
    }
    // Free memory for results
    for (int col = 0; col < NUM_COL; ++col) {
        delete[] results[col];
    }
}

int main(int argc, char *argv[]) {
    std::string indiceFile;
    bool isUpperBound = false;
    if (!strcmp(argv[1], "upper")) {
        isUpperBound = true;
        indiceFile = "upper_bound.txt";
    } else {
        indiceFile = "lower_bound.txt";
    }

    // Get configurations
    constexpr size_t dims[1] = {PARTITION_SIZE};
    SZ::Config conf({dims[0]});
    conf.cmprAlgo = SZ::ALGO_INTERP_LORENZO;
    conf.errorBoundMode = SZ::EB_REL; // refer to def.hpp for all supported error bound mode
    conf.relErrorBound = 1E-4;        // rel error bound 1e-4

    // Load indices
    std::vector<int> all_ids = load_indices((indiceDir + indiceFile).c_str());

    // Start timer
    SZ::Timer timer(true);

    // Get partition file names and ids
    auto [partitions, partitionOffsets] = get_partitions_and_ids(all_ids);
    double search_time = timer.stop();

    // Run query
    double decompress_time = 0.0;
    double lookup_time = 0.0;
    if (isUpperBound) {
        upper_bound(partitions, partitionOffsets, colNames, conf, decompress_time, lookup_time);
    } else {
        lower_bound(partitions, partitionOffsets, colNames, conf, decompress_time, lookup_time);
    }
    // Stop timer and measure overall retrieval time
    double overall_time = timer.stop();

    printf("Search time = %f seconds.\n", search_time);
    printf("Decompression time = %f seconds.\n", decompress_time);
    printf("Lookup time = %f seconds.\n", lookup_time);
    printf("Overall retrieval time = %f seconds.\n", overall_time);
    return 0;
}